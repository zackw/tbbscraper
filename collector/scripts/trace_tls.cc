/* Trace TLS sessions and report the sequence of records.
 *
 * Copyright © 2014 Zack Weinberg
 * Portions from tcpflow, copyright Simson Garfinkel <simsong@acm.org>
 * and Jeremy Elson <jelson@circlemud.org>.
 *
 * Due to code from tcpflow, this file is licensed under the
 * GNU General Public License version 3 (specifically).
 * https://www.gnu.org/licenses/gpl.html
 * There is NO WARRANTY.
 *
 *     trace_tls <uid> <output-base> [<pcap filter expression>]
 *
 * records all TLS-over-TCP-over-IPv4 sessions initiated by processes
 * with uid <uid>, writing them to files whose names begin with
 * <output-base>.  This program does not presently support IPv6 or
 * DTLS.  An additional filter expression may be provided to further
 * constrain traffic traced.
 *
 * Due to internal use of iptables and nflog, this program is
 * presently Linux-specific.
 *
 * Time is divided into epochs; a new epoch starts every time the program
 * receives a SIGUSR1.  Output is to files named
 *
 *     ooooooo_eeee
 *
 * where oooooo is the output-base on the command line, and eee is the
 * epoch number (starts at 1, increments each time SIGUSR1 is received).
 *
 * Within each file, there is one line per TLS record, taking the form
 *
 *     ssss d yy v.v llll tt.tttt
 *
 * where: ssss is the TCP session number, d is the direction of the
 * packet ('C' sent by client, 'S' sent by server); yy is the record
 * type code as a decimal number (0-255; should always be one of 20,
 * 21, 22, or 23 -- ChangeCipherSpec, Alert, Handshake, AppData
 * respectively); v.v is the TLS version code as two decimal numbers
 * (0-255 each, normally 3 [0123]); llll is the length of this record
 * as declared in the record header; and tt.tttt is the time in
 * milliseconds since the previous packet with the same source.
 * Packets and records are not decoded any further than necessary to
 * produce this summary; in particular, we make no effort to decode
 * the handshake or validate MACs on encrypted data (not that we
 * could, anyway).  Any packets that occur before the first TLS
 * Handshake record appears are not recorded; normally this will only
 * be the TCP handshake.
 *
 * TCP session numbers are independent of epoch numbers and are not
 * reused.
 *
 * The very first line associated with a new session is different; it
 * looks like
 *
 *     ssss C SS c.c.c.c s.s.s.s YYYY-MM-DD HH:MM:SS.ssssssss
 *
 * where 'C SS' is literal (C for client, SS for SYN); c.c.c.c and
 * s.s.s.s are the IPv4 addresses of client and server respectively,
 * and YYYY-MM-DD HH:MM:SS.ssssssss is the absolute UTC time of
 * transmission of the first TLS record for this session (which will
 * be described on the next line).
 *
 * If a session continues across epochs, the first time it appears in
 * a new epoch, the above special line will be repeated, with SS
 * replaced by CC (for CONTINUED) and the timestamp indicating the
 * absolute time of the first packet in the new epoch.
 *
 * If a connection closes cleanly (TCP FIN), that will be recorded as
 *
 *     s FF 0.0 0 tt.tttt
 *
 * where s and tt.tttt are as for other records, and FF 0.0 0 is literal.
 *
 * The program closes all files and exits cleanly if it receives
 * SIGHUP, SIGINT, or SIGTERM.
 */

#include <functional> // std::hash
#include <string>
#include <unordered_map>
#include <vector>

#include <errno.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <pwd.h>
#include <unistd.h>

#include <pcap/pcap.h>

using std::string;
using std::unordered_map;
using std::vector;

#if defined __GNUC__ && __GNUC__ >= 4
#define NORETURN void __attribute__((noreturn))
#define PRINTFLIKE __attribute__((format(printf,1,2)))
#else
#define NORETURN void
#define PRINTFLIKE /*nothing*/
#endif

namespace {

// Far below, but called by several of the "general utilities".
NORETURN cleanup_and_exit(int);
bool suppress_all_cleanups;

//
// Error reporting.
//

const char *progname;

NORETURN
fatal(const char *msg)
{
  fprintf(stderr, "%s: %s\n", progname, msg);
  cleanup_and_exit(1);
}

NORETURN
fatal_perror(const char *msg)
{
  fprintf(stderr, "%s: %s: %s\n", progname, msg, strerror(errno));
  cleanup_and_exit(1);
}

NORETURN
fatal_pcap_error(const char *msg, pcap_t *pc)
{
    fprintf(stderr, "%s: %s: %s\n", progname, msg, pcap_geterr(pc));
    cleanup_and_exit(1);
}

PRINTFLIKE NORETURN
fatal_printf(const char *msg, ...)
{
  va_list ap;
  fprintf(stderr, "%s: ", progname);
  va_start(ap, msg);
  vfprintf(stderr, msg, ap);
  va_end(ap);
  putc('\n', stderr);
  cleanup_and_exit(1);
}

PRINTFLIKE NORETURN
fatal_eprintf(const char *msg, ...)
{
  int err = errno;
  fprintf(stderr, "%s: ", progname);
  va_list ap;
  va_start(ap, msg);
  vfprintf(stderr, msg, ap);
  va_end(ap);
  fprintf(stderr, ": %s\n", strerror(err));
  cleanup_and_exit(1);
}

//
// General utilities.
//

inline bool
is_all_digits(const char *s)
{
    for (; *s; s++)
        if (*s < '0' || *s > '9')
            return false;
    return true;
}

// Decoded packet headers and related data structures.  All fields are in
// _host_ byte order, and may not be in the same order as they appear on
// the wire.  We only decode the fields we care about.

inline uint16_t get_be16(const uint8_t *p)
{
    return (uint16_t(p[0]) << 8) | uint16_t(p[1]);
}

inline uint32_t get_be32(const uint8_t *p)
{
    return ((uint32_t(p[0]) << 24) |
            (uint32_t(p[1]) << 16) |
            (uint32_t(p[2]) <<  8) |
            (uint32_t(p[3]) <<  0));
}

struct hdr_ipv4
{
    bool      valid : 1;
    bool      fragmented : 1;
    bool      more_fragments : 1;
    uint8_t   header_len; // in *bytes*
    uint8_t   protocol;

    uint16_t  packet_len;
    uint16_t  identification;
    uint16_t  fragment_offset; // in 64-bit units
    uint16_t  header_checksum;

    uint32_t  source_addr;
    uint32_t  dest_addr;
    // options are not decoded

    hdr_ipv4(const uint8_t *frame)
    {
        memset(this, 0, sizeof *this);

        uint8_t v_ihl = frame[0];
        if ((v_ihl & 0xF0) != 0x40)
            return; // not an IPv4 packet?!

        // wire length field is in 32-bit words
        header_len = (v_ihl & 0x0F) * 4;
        if (header_len < 20)
            return; // too short

        // total length field is in bytes
        packet_len = get_be16(frame+2);
        if (packet_len < 20)
            return; // too short

        // validate header checksum
        header_checksum = get_be16(frame+10);
        uint32_t checksum = 0;
        for (size_t i = 0; i < header_len; i += 2)
            checksum += get_be16(frame + i);
        // fold the carry back in
        checksum = (checksum & 0xFFFF) + ((checksum & 0xFF0000) >> 16);
        if (~checksum)
            return; // checksum invalid

        fragment_offset = get_be16(frame+6);
        if (fragment_offset == 0x8000)
            return; // reserved bit set, packet invalid
        else if (fragment_offset == 0x4000)
            fragment_offset = 0; // DF bit is set, no fragments
        else
        {
            fragmented = true;
            more_fragments = fragment_offset & 0x2000;
            fragment_offset &= 0x1FFF;
        }

        // ok, everything we check is valid
        valid = true;

        protocol = frame[9];
        identification = get_be16(frame+4);
        source_addr = get_be32(frame+12);
        dest_addr = get_be32(frame+16);
    }
};

// For what we're doing, we don't care about the window or most of the
// flags.  Unlike at the IP level, we don't bother with the checksum
// validation.  (The kernel should've thrown away packets with invalid
// checksums already, and constructing the pseudo-header is too much
// effort.)
struct hdr_tcp
{
    uint16_t source_port;
    uint16_t dest_port;
    uint32_t seqno;
    uint32_t ackno;
    uint8_t data_offset; // in BYTES

    bool valid : 1;
    bool syn : 1;
    bool fin : 1;
    bool rst : 1;
    bool ack : 1;

    // TCP and IP headers are 1:1.
    hdr_tcp(const uint8_t *frame, const hdr_ip &iph)
    {
        memset(this, 0, sizeof *this);

        const uint8_t *tf = frame + iph.header_len;
        if (iph.header_len + 20 < iph.packet_len)
            return; // invalid: not enough room for any TCP header

        data_offset = ((tf[12] & 0xF0) >> 4) * 2;
        if (data_offset < 20 || data_offset > 60 ||
            iph.header_len + data_offset < iph.packet_len)
            return; // invalid: not enough room for this TCP header

        source_port = get_be16(tf + 0);
        dest_port   = get_be16(tf + 2);
        seqno       = get_be32(tf + 4);
        ackno       = get_be32(tf + 8);

        uint8_t flags = tf[13];

        fin = flags & 0x01;
        syn = flags & 0x02;
        rst = flags & 0x04;
        ack = flags & 0x10;
        valid = true;
    }
};

struct hdr_tls
{
    uint8_t  type;
    uint8_t  version_maj;
    uint8_t  version_min;
    uint16_t length;

    // TLS records are not in any simple relationship with TCP/IP
    // packets.  We do not attempt to decode beyond the record
    // protocol.
    hdr_tls(const uint8_t *data)
    {
        type = data[0];
        version_maj = data[1];
        version_min = data[2];
        length = get_be16(data+3);
    }
};

// The 4-tuple identifying an active TCP connection.

struct tcp_connection_id
{
    uint32_t client_addr;
    uint32_t server_addr;
    uint16_t client_port;
    uint16_t server_port;

    bool operator==(const tcp_connection_id& o)
    {
        return (client_addr == o.client_addr &&
                server_addr == o.server_addr &&
                client_port == o.client_port &&
                server_port == o.server_port);
    }
};

} // anonymous namespace

// This (plus operator==) is the incantation to make tcp_connection_id
// usable as an unordered_map key.
namespace std {
template <>
struct hash<tcp_connection_id>
{
    std::size_t operator()(const tcp_connection& k) const
    {
        // taken from http://burtleburtle.net/bob/c/lookup3.c (final())
        // conveniently, we have exactly 3 32-bit quantities to hash.
        uint32_t a = k.client_addr;
        uint32_t b = k.server_addr;
        uint32_t c = (k.client_port | (uint32_t(k.server_port) << 16));

#define rot(x,k) (((x)<<(k)) | ((x)>>(32-(k))))
        c ^= b; c -= rot(b,14);
        a ^= c; a -= rot(c,11);
        b ^= a; b -= rot(a,25);
        c ^= b; c -= rot(b,16);
        a ^= c; a -= rot(c, 4);
        b ^= a; b -= rot(a,14);
        c ^= b; c -= rot(b,24);
#undef rot

        return c;
    }
};
}

namespace {

//
// Child process management.  Used for invocations of iptables.
//

char *const *child_envp;
sigset_t child_sigmask;

void
fatal_if_unsuccessful_child(const char *child, int status)
{
  if (status == 0)
    return;
  if (status == -1)
    fatal_eprintf("invoking %s", child);

  if (WIFEXITED(status))
    fatal_printf("%s: unsuccessful exit %d", child, WEXITSTATUS(status));
  if (WIFSIGNALED(status))
    fatal_printf("%s: %s%s", child, strsignal(WTERMSIG(status)),
                 WCOREDUMP(status) ? " (core dumped)" : "");

  fatal_printf("%s: unexpected status %04x (neither exit nor fatal signal)",
               child, status);
}

// A process which is setuid - that is, getuid() != 0, geteuid() == 0 -
// behaves differently than one which holds _only_ root credentials.
// We don't want iptables acting up because of that.  This is done
// only for child processes because one of the differences is that a
// setuid program can be killed by the invoking (real) UID, which we
// do want to allow.
void
become_only_root(void)
{
  if (geteuid() != 0)
    fatal("must be run as root");

  /* Discard all supplementary groups. */
  if (setgroups(0, 0))
    fatal_perror("setgroups");

  /* Set the real GID and UID to zero. This _should_ also set the
     saved GID and UID, divorcing the process completely from its
     original invoking user. */
  if (setgid(0))
    fatal_perror("setgid");
  if (setuid(0))
    fatal_perror("setuid");
}

pid_t
xspawnvp(const char *const *argv)
{
  fflush(0);
  pid_t child = fork();
  if (child == -1)
    fatal_perror("fork");
  if (child != 0)
    return child; /* to the parent */

  // We are the child.  The parent has arranged for it to be safe for
  // us to write to stderr under error conditions, but the cleanup
  // handler should not do anything.
  suppress_all_cleanups = true;

  // stdin and stdout point to /dev/null, stderr left alone.
  if (close(0))
      fatal_perror("close");
  if (open("/dev/null", O_RDONLY) != 0)
      fatal_perror("open");

  if (close(1))
      fatal_perror("close");
  if (open("/dev/null", O_WRONLY) != 1)
      fatal_perror("open");

  become_only_root();
  if (sigprocmask(SIG_SETMASK, &child_sigmask, 0))
    fatal_perror("sigprocmask");

  execvpe(argv[0], (char *const *)argv, child_envp);
  fatal_perror("execvpe");
}

void
runv(const char *const *argv)
{
    pid_t pid = xspawnvp(argv);
    int status;
    if (waitpid(pid, &status, 0) != pid)
        fatal_perror("waitpid");
    fatal_if_unsuccessful_child(argv[0], status);
}
#define run(...) runv((const char *const []){ __VA_ARGS__, 0 })


//
// Master control.
//

bool iptables_established;
string m_uid;
string m_log_iface;

void
establish_iptables(const char *to_monitor)
{
    // If to_monitor is not a number, it's a username.
    if (is_all_digits(to_monitor))
        m_uid = to_monitor;
    else
    {
        struct passwd *pw = getpwnam(to_monitor);
        if (!pw)
            fatal_perror(to_monitor);

        m_uid = std::to_string(pw->pw_uid);
    }

    m_log_iface = "nflog:";
    m_log_iface.append(m_uid);

    // Logic borrowed from http://wiki.wireshark.org/CaptureSetup/NFLOG .
    // As far as I can tell, it is impossible to do this atomically.
    const char *uid = m_uid.c_str();
    run("iptables", "-A", "OUTPUT",
        "-m", "owner", "--uid-owner", uid, "-j", "CONNMARK", "--set-mark", uid);
    run("iptables", "-A", "OUTPUT",
        "-m", "connmark", "--mark", uid, "-j", "NFLOG", "--nflog-group", uid);
    run("iptables", "-A", "INPUT",
        "-m", "connmark", "--mark", uid, "-j", "NFLOG", "--nflog-group", uid);

    iptables_established = true;
}

void
clear_iptables(void)
{
    const char *uid = m_uid.c_str();
    run("iptables", "-D", "INPUT",
        "-m", "connmark", "--mark", uid, "-j", "NFLOG", "--nflog-group", uid);
    run("iptables", "-D", "OUTPUT",
        "-m", "connmark", "--mark", uid, "-j", "NFLOG", "--nflog-group", uid);
    run("iptables", "-D", "OUTPUT",
        "-m", "owner", "--uid-owner", uid, "-j", "CONNMARK", "--set-mark", uid);

    iptables_established = false;
}

NORETURN
cleanup_and_exit(int rc)
{
    if (!suppress_all_cleanups)
    {
        // Don't go into infinite recursion if we wind up back here.
        suppress_all_cleanups = true;

        if (iptables_established)
        {
            clear_iptables();
            iptables_established = false;
        }
    }
    exit(rc);
}

} // anonymous namespace

int main(int argc, char **argv, char **envp)
{
    progname = strrchr(argv[0], '/');
    if (progname)
        progname++;
    else
        progname = argv[0];

    if (argc < 3)
    {
        fprintf(stderr, "usage: %s <interface> <output-base> [<pcap filter>]",
                progname);
        return 2;
    }

    std::string pcap_filter;
    if (argc >= 4)
        for (int i = 3; i < argc; i++)
        {
            if (i > 3)
                pcap_filter.append(1, ' ');
            pcap_filter.append(argv[i]);
        }

    prepare_child_env(envp);
    int sfd = prepare_signals();

    establish_iptables(argv[1]);

    return 0;
}

// Local Variables:
// c-basic-offset: 4
// c-file-offsets: ((innamespace . 0) (substatement-open . 0))
// End:
