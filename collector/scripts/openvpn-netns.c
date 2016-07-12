/* Establish network namespaces that use OpenVPN for all communication.
 *
 * Copyright © 2014 Zack Weinberg
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * There is NO WARRANTY.
 *
 *     openvpn-netns namespace config-file [args...]
 *
 * brings up an OpenVPN tunnel which network namespace NAMESPACE will
 * use for communication.  NAMESPACE must already exist.  (The program
 * 'tunnel-ns' sets up namespaces appropriately.)  CONFIG-FILE is an
 * OpenVPN configuration file, and any ARGS will be appended to the
 * OpenVPN command line.
 *
 * This program expects to be run with both stdin and stdout connected
 * to pipes.  When it detects that the namespace is ready for use, it
 * will write the string "READY\n" to its stdout and then close it.
 * It expects that nothing will be written to its stdin (anything that
 * *is* written will be read and discarded), but when stdin is closed,
 * it will terminate the OpenVPN client, tear down the network
 * namespace (and terminate all processes still in there), and exit.
 *
 * Error messages, and any output from the OpenVPN client, will be
 * written to stderr.  One may wish to include "--verb 0" in ARGS to
 * make the client less chatty.
 *
 * This program must be installed setuid root.
 *
 * This program makes extensive use of Linux-specific network stack
 * features.  A port to a different OS might well entail a complete
 * rewrite.  Apart from that, C99 and POSIX.1-2001 features are used
 * throughout.  It also requires dirfd, strdup, and strsignal, from
 * POSIX.1-2008; execvpe, pipe2, and vasprintf, from the shared
 * BSD/GNU extension set; and the currently Linux-specific signalfd
 * and getauxval.
 */

#define _GNU_SOURCE 1
#define _FILE_OFFSET_BITS 64 /* large directory readdir(), large rlimits */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <sys/auxv.h>
#include <sys/resource.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <limits.h>
#include <poll.h>
#include <regex.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#if defined __GNUC__ && __GNUC__ >= 4
#define NORETURN void __attribute__((noreturn))
#define PRINTFLIKE __attribute__((format(printf,1,2)))
#define UNUSED(arg) arg __attribute__((unused))
#else
#define NORETURN void
#define PRINTFLIKE /*nothing*/
#define UNUSED(arg) arg
#endif

/* Global state. */

static const char *progname;
static const char *full_progname;

static const char *const *child_env;
static sigset_t child_sigmask;

static bool is_controller;
static bool controller_cleanups;
static bool already_closed_stdout;

/* controller cleanups need to know: */
static pid_t cl_ovpn_pid;

/* Error reporting. */

static NORETURN cleanup_and_exit(int);
static NORETURN usage(void);

static NORETURN
fatal(const char *msg)
{
  fprintf(stderr, "%s: %s\n", progname, msg);
  cleanup_and_exit(1);
}

static NORETURN
fatal_perror(const char *msg)
{
  fprintf(stderr, "%s: %s: %s\n", progname, msg, strerror(errno));
  cleanup_and_exit(1);
}

static NORETURN
fatal_regerror(const char *msg, int errcode, const regex_t *offender)
{
  size_t req = regerror(errcode, offender, 0, 0);
  char *errbuf = malloc(req);
  if (!errbuf)
    fatal_perror("malloc");
  regerror(errcode, offender, errbuf, req);
  fprintf(stderr, "%s: %s: %s\n", progname, msg, errbuf);
  exit(1);
}

static PRINTFLIKE NORETURN
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

static PRINTFLIKE NORETURN
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

static void
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

/* Utilities. */

#define startswith(x, y) (!strncmp((x), (y), sizeof(y) - 1))

static int
compar_str(const void *a, const void *b)
{
  return strcmp(*(const char *const *)a, *(const char *const *)b);
}

static char * PRINTFLIKE
xasprintf(const char *fmt, ...)
{
  char *rv;
  va_list ap;
  va_start(ap, fmt);
  if (vasprintf(&rv, fmt, ap) == -1)
    fatal_perror("asprintf");
  va_end(ap);
  return rv;
}

static void *
xreallocarray(void *optr, size_t nmemb, size_t size)
{
  /* s1*s2 <= SIZE_MAX if both s1 < K and s2 < K where K = sqrt(SIZE_MAX+1) */
  const size_t MUL_NO_OVERFLOW = ((size_t)1) << (sizeof(size_t) * 4);

  if ((nmemb >= MUL_NO_OVERFLOW || size >= MUL_NO_OVERFLOW) &&
      nmemb > 0 && SIZE_MAX / nmemb < size) {
    errno = ENOMEM;
    fatal_perror("malloc");
  }

  void *rv = realloc(optr, size * nmemb);
  if (!rv)
    fatal_perror("malloc");
  return rv;
}

static long long
xstrtonum(const char *str, long long minval, long long maxval,
          const char *msgprefix)
{
  if (minval > maxval)
    fatal_printf("xstrtonum: misuse: minval(%lld) > maxval(%lld)",
                 minval, maxval);

  long long rv;
  char *endp;
  errno = 0;
  rv = strtoll(str, &endp, 10);
  if (endp == str || *endp != '\0')
    fatal_printf("%s: '%s': invalid number", msgprefix, str);
  else if (errno)
    fatal_eprintf("%s: '%s'", msgprefix, str);
  else if (rv < minval)
    fatal_printf("%s: '%s': too small (minimum %lld)", msgprefix, str, minval);
  else if (rv > maxval)
    fatal_printf("%s: '%s': too large (maximum %lld)", msgprefix, str, maxval);

  return rv;
}

static char *
xreadall(int fd)
{
  size_t nread = 0;
  size_t alloc = BUFSIZ;
  char *buf = xreallocarray(0, alloc, 1);

  for (;;) {
    ssize_t count = read(fd, buf + nread, alloc - nread);
    if (count == 0)
      break;
    if (count < 0)
      fatal_perror("read");

    nread += (size_t)count;
    while (nread >= alloc) {
      alloc *= 2;
      buf = xreallocarray(buf, alloc, 1);
    }
  }

  buf = xreallocarray(buf, nread+1, 1);
  buf[nread] = '\0';
  return buf;
}

static const char *
must_getenv(const char *var)
{
  const char *val = getenv(var);
  if (!val || !*val) {
    fprintf(stderr, "%s: %s: required variable not set in environment\n",
            progname, var);
    fprintf(stderr, "%s: --- begin environment dump ---\n", progname);
    for (const char *const *ep = (const char *const *)environ;
         *ep;
         ep++)
      fprintf(stderr, "%s\n", *ep);
    fatal("--- end environment dump ---");
  }
  return val;
}

static char *
strip(char *text)
{
  char *p = text;
  while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')
    p++;

  if (*p == '\0')
    return p;

  char *q = p + strlen(p) - 1;
  while (*q == ' ' || *q == '\t' || *q == '\r' || *q == '\n')
    q--;
  q[1] = '\0';
  return p;
}

/* Compute the CIDR /nnn form of a dotted-quad netmask. */

static inline int
count_leading_zeros(uint32_t x)
{
#if defined __GNUC__ && __GNUC__ >= 4
  return __builtin_clz(x);
#else
  if (x == 0) return 32;
  int n = 0;
  if ((x & 0xFFFF0000u) == 0) { n += 16; x <<= 16; }
  if ((x & 0xFF000000u) == 0) { n +=  8; x <<=  8; }
  if ((x & 0xF0000000u) == 0) { n +=  4; x <<=  4; }
  if ((x & 0xC0000000u) == 0) { n +=  2; x <<=  2; }
  if ((x & 0x80000000u) == 0) { n +=  1; }
  return n;
#endif
}

static inline int
count_trailing_zeros(uint32_t x)
{
#if defined __GNUC__ && __GNUC__ >= 4
  return __builtin_ctz(x);
#else
  x = (x & (-(int32_t)x));
  int n = 32;
  if (v) n--;
  if (v & 0x0000FFFFu) n -= 16;
  if (v & 0x00FF00FFu) n -= 8;
  if (v & 0x0F0F0F0Fu) n -= 4;
  if (v & 0x33333333u) n -= 2;
  if (v & 0x55555555u) n -= 1;
  return n;
#endif
}

static int
mask2cidr(const char *netmask)
{
  struct in_addr addr;
  if (inet_pton(AF_INET, netmask, &addr) != 1)
    fatal_printf("%s: not a valid dotted-quad address", netmask);

  uint32_t mask = ntohl(addr.s_addr);

  /* To be a *netmask*, the number we get from inet_pton must be of
     the form 1...10...0, i.e. there must be a single point in the
     bit representation where it changes from all-1 to all-0. */
  int leading_ones   = count_leading_zeros(~mask);
  int trailing_zeros = count_trailing_zeros(mask);

  if (leading_ones != 32 - trailing_zeros)
    fatal_printf("%s (%08x: %dlo, %dtz): not a valid netmask",
                 netmask, mask, leading_ones, trailing_zeros);

  return leading_ones;
}

/* Vectors. */

typedef struct strvec {
  const char **vec;
  size_t alloc;
  size_t used;
} strvec;

typedef struct pidvec {
  pid_t *vec;
  size_t alloc;
  size_t used;
} pidvec;

static void
strvec_append(strvec *v, const char *val)
{
  if (v->used >= v->alloc) {
    if (v->alloc == 0)
      v->alloc = 8;
    else
      v->alloc *= 2;

    v->vec = xreallocarray(v->vec, v->alloc, sizeof(char *));
  }

  v->vec[v->used++] = val;
}

static void
pidvec_clear(pidvec *v)
{
  free (v->vec);
  memset(v, 0, sizeof(pidvec));
}

static void
pidvec_append(pidvec *v, pid_t val)
{
  if (v->used >= v->alloc) {
    if (v->alloc == 0)
      v->alloc = 8;
    else
      v->alloc *= 2;

    v->vec = xreallocarray(v->vec, v->alloc, sizeof(char *));
  }

  v->vec[v->used++] = val;
}

static void
pidvec_from_text(pidvec *v, char *text, const char *msgprefix)
{
  char *p = text;
  char *token;
  while ((token = strsep(&p, " \t\n"))) {
    if (*token)
      pidvec_append(v, (pid_t)xstrtonum(token, 0, INT_MAX, msgprefix));
  }
  free (text);
}

static void
pidvec_kill(pidvec *v, int signo)
{
  for (size_t i = 0; i < v->used; i++)
    kill(v->vec[i], signo);  /* errors deliberately ignored */
}

/* Child process management. */

/* A process which is setuid - that is, getuid() != 0, geteuid() == 0 -
   behaves differently than one which holds _only_ root credentials.
   We don't want openvpn or the scripts acting up because of that.
   This is done only for child processes because one of the
   differences is that a setuid program can be killed by the
   invoking (real) UID, which we do want to allow.  */
static void
become_only_root(void)
{
  /* Don't need to do this if it's already been done. */
  if (!is_controller)
    return;

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

static pid_t
spawn_with_redir(const char *const *argv, int child_stdin, int child_stdout)
{
  fflush(0);
  pid_t child = fork();
  if (child == -1)
    fatal_perror("fork");
  if (child != 0)
    return child; /* to the parent */

  /* We are the child.  The parent has arranged for it to be safe for
     us to write to stderr under error conditions, but the cleanup
     handler should not do anything. */
  controller_cleanups = false;

  /* Child-side stdin and stdout redirections. */
  if (child_stdin != 0) {
    if (close(0))
      fatal_perror("close");

    if (child_stdin < 0) {
      if (open("/dev/null", O_RDONLY) != 0)
        fatal_perror("open");
    } else {
      if (dup(child_stdin) != 0)
        fatal_perror("dup");
    }
  }

  if (child_stdout != 1) {
    if (close(1))
      fatal_perror("close");
    if (child_stdout < 1) {
      if (open("/dev/null", O_WRONLY) != 1)
        fatal_perror("open");
    } else {
      if (dup(child_stdout) != 1)
        fatal_perror("dup");
    }
  }

  become_only_root();

  if (sigprocmask(SIG_SETMASK, &child_sigmask, 0))
    fatal_perror("sigprocmask");

  execvpe(argv[0], (char *const *)argv, (char *const *)child_env);
  fatal_perror("execvpe");
}

static pid_t
xspawnvp(const char *const *argv)
{
  return spawn_with_redir(argv, -1, 2);
}

static void
runv(const char *const *argv)
{
  pid_t pid = xspawnvp(argv);
  int status;
  if (waitpid(pid, &status, 0) != pid)
    fatal_perror("waitpid");
  fatal_if_unsuccessful_child(argv[0], status);
}
#define run(...) runv((const char *const []){ __VA_ARGS__, 0 })

static char *
runv_get_output(const char *const *argv)
{
  int pipefds[2];
  if (pipe2(pipefds, O_CLOEXEC))
    fatal_perror("pipe");

  pid_t pid = spawn_with_redir(argv, -1, pipefds[1]);
  close(pipefds[1]);

  char *output = xreadall(pipefds[0]);
  close(pipefds[0]);

  int status;
  if (waitpid(pid, &status, 0) != pid)
    fatal_perror("waitpid");
  fatal_if_unsuccessful_child(argv[0], status);

  return output;
}
#define run_get_output(...) \
  runv_get_output((const char *const []){ __VA_ARGS__, 0 })

static void
runv_get_output_pids(pidvec *v, const char *const *argv)
{
  char *buf = runv_get_output(argv);
  pidvec_from_text(v, buf, argv[0]);
}

/* 'Scripts' executed from inside openvpn. */

static void
maybe_config_dns(const char *namespace, char **envp)
{
  strvec nservers;
  memset(&nservers, 0, sizeof(strvec));

  regex_t dns_opt;
  regmatch_t match[2];
  int rerr = regcomp(&dns_opt,
                     "^foreign_option_[0-9]+="
                     "dhcp-option[ \t]+DNS[ \t]+"
                     "([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)$",
                     REG_EXTENDED);
  if (rerr)
    fatal_regerror("regcomp", rerr, &dns_opt);

  for (size_t i = 0; envp[i]; i++) {
    rerr = regexec(&dns_opt, envp[i], 2, match, 0);
    if (!rerr) {
      const char *ns = xasprintf("nameserver %.*s\n",
                                 match[1].rm_eo - match[1].rm_so,
                                 envp[i] + match[1].rm_so);
      strvec_append(&nservers, ns);
    } else if (rerr != REG_NOMATCH)
      fatal_regerror("regexec", rerr, &dns_opt);
  }

  if (!nservers.used)
    return;

  const char *path = xasprintf("/etc/netns/%s/resolv.conf", namespace);
  FILE *rc = fopen(path, "w");
  if (!rc)
    fatal_perror(path);
  fprintf(rc, "# name servers for %s\n", namespace);
  for (size_t i = 0; i < nservers.used; i++)
    fputs(nservers.vec[i], rc);
  if (ferror(rc) || fclose(rc))
    fatal_perror(path);
}

static void
config_ipv4_routes(const char *tun_dev)
{
  /* Arbitrary limit of 1000 non-default routes. */
  for (unsigned int i = 0; i < 1000; i++) {
    const char *rgw_i = xasprintf("route_gateway_%d", i);
    const char *rnm_i = xasprintf("route_netmask_%d", i);
    const char *rnw_i = xasprintf("route_network_%d", i);
    const char *gateway = getenv(rgw_i);
    const char *netmask = getenv(rnm_i);
    const char *network = getenv(rnw_i);
    if (!gateway || !netmask || !network ||
        !*gateway || !*netmask || !*network)
      break;
    network = xasprintf("%s/%u", network, mask2cidr(netmask));
    run("ip", "route", "add", network, "via", gateway, "dev", tun_dev);
  }
  const char *dgateway = getenv("route_vpn_gateway");
  if (dgateway) {
    run("ip", "route", "add", "default", "via", dgateway, "dev", tun_dev);
  }
}

static void
config_ipv6_routes(const char *tun_dev)
{
  /* Arbitrary limit of 1000 non-default routes. */
  for (unsigned int i = 0; i < 1000; i++) {
    const char *rgw_i = xasprintf("route_ipv6_gateway_%d", i);
    const char *rnw_i = xasprintf("route_ipv6_network_%d", i);
    const char *gateway = getenv(rgw_i);
    const char *network = getenv(rnw_i);
    if (!gateway || !network || !*gateway || !*network)
      break;
    run("ip", "route", "add", network, "via", gateway, "dev", tun_dev);
  }
}

static void
do_up_script(int argc, char **argv, char **envp)
{
  if (argc < 4) {
    fprintf(stderr,
            "INTERNAL-ONLY usage: %s --as-up-script namespace parent-pid ...\n",
            progname);
    exit(2);
  }

  const char *namespace = argv[2];
  const char *ppid_s = argv[3];
  child_env = (const char *const *)envp;

  /* Process ID of the parent instance of this program, above the
     openvpn client, passed down on command line. */
  pid_t ppid = (pid_t)xstrtonum(ppid_s, 0, INT_MAX, "parent process ID");

  /* Arguments passed down in environment variables from openvpn.
     Most of these are not needed until we get to do_up_inside_ns,
     which runs in another process, but sanity-checking everything now
     avoids doing a bunch of work that will just have to be undone. */
  const char *tun_dev = must_getenv("dev");
  must_getenv("tun_mtu");
  must_getenv("route_vpn_gateway");
  must_getenv("ifconfig_local");

  /* Make sure we create everything with the correct permissions.
     This system call cannot fail.  */
  umask(022);

  maybe_config_dns(namespace, envp);

  run("ip", "link", "set", "dev", tun_dev, "netns", namespace);
  run("ip", "netns", "exec", namespace,
      full_progname, "--as-up-inside-ns", namespace);

  /* The namespace is now ready for use; signal the parent instance. */
  if (kill(ppid, SIGUSR1))
    fatal_perror("signaling parent instance");
}

static void
do_up_inside_ns(int argc, char **argv, char **envp)
{
  if (argc != 3) {
    fprintf(stderr, "INTERNAL-ONLY usage: %s --as-up-inside-ns namespace ...\n",
            progname);
    exit(2);
  }

  const char *namespace = argv[2];
  child_env = (const char *const *)envp;

  /* Arguments passed down in environment variables from openvpn. */
  const char *tun_dev  = must_getenv("dev");
  const char *tun_mtu  = must_getenv("tun_mtu");
  const char *if_local = must_getenv("ifconfig_local");
  const char *if_nmask = getenv("ifconfig_netmask");
  const char *if_bcast = getenv("ifconfig_broadcast");

  /* Sanity check. */
  const char *actual_namespace =
    strip(run_get_output("ip", "netns", "identify", xasprintf("%d", getpid())));
  if (strcmp(actual_namespace, namespace))
    fatal_printf("not running inside expected namespace - got '%s', want '%s'",
                 actual_namespace, namespace);

  if (if_nmask) {
    /* 'ip addr add' wants a CIDR-format netmask. */
    if_local = xasprintf("%s/%u", if_local, mask2cidr(if_nmask));
    if (if_bcast) {
      run("ip", "addr", "add", "dev", tun_dev, "local", if_local,
          "broadcast", if_bcast);
    } else {
      run("ip", "addr", "add", "dev", tun_dev, "local", if_local);
    }
  } else {
    /* If we don't have a netmask, this is a point-to-point hop and we
       had better have a remote.   Broadcast, if any, doesn't make sense
       and is ignored. */
    const char *if_remote = must_getenv("ifconfig_remote");
    run("ip", "addr", "add", "dev", tun_dev, "local", if_local,
        "peer", if_remote);
  }
  run("ip", "link", "set", "dev", tun_dev, "mtu", tun_mtu, "up");

  /* If we got an IPv6 address, configure that too. */
  const char *if6_local = getenv("ifconfig_ipv6_local");
  const char *if6_cidr  = getenv("ifconfig_ipv6_netbits");
  if (if6_local && if6_cidr) {
    if6_local = xasprintf("%s/%s", if6_local, if6_cidr);
    run("ip", "addr", "add", "dev", tun_dev, "local", if6_local);
    config_ipv6_routes(tun_dev);
  }

  /* This sets the default route, so do it last. */
  config_ipv4_routes(tun_dev);
}

static void
do_down_script(int argc, char **argv, char **envp)
{
  if (argc < 3) {
    fprintf(stderr, "INTERNAL-ONLY usage: %s --as-down-script namespace ...\n",
            progname);
    exit(2);
  }

  const char *namespace = argv[2];
  child_env = (const char *const *)envp;

  /* When this is called, the OpenVPN daemon has already closed its end of
     the tunnel device, which makes the kernel automatically tear down all
     of the tunnel-interface-related state that we would otherwise have
     to clear with more "ip" commands.  All that remains to do is kill
     anything still running in the namespace, and delete resolv.conf. */

  const char *nsdir = xasprintf("/etc/netns/%s", namespace);
  struct stat st;
  if (lstat(nsdir, &st))
    fatal_perror(nsdir);
  if (!S_ISDIR(st.st_mode))
    fatal_printf("%s: not a directory", nsdir);

  pidvec to_kill;
  memset(&to_kill, 0, sizeof(pidvec));

  const char *ipcmd[] = { "ip", "netns", "pids", namespace, 0 };
  runv_get_output_pids(&to_kill, ipcmd);
  if (to_kill.used) {
    pidvec_kill(&to_kill, SIGTERM);
    pidvec_clear(&to_kill);

    sleep(5);
    runv_get_output_pids(&to_kill, ipcmd);
    pidvec_kill(&to_kill, SIGKILL);
  }

  const char *path = xasprintf("/etc/netns/%s/resolv.conf", namespace);
  (void) unlink(path);
}

/* Master control. */

/* Infuriatingly, Linux refuses to adopt closefrom(). This is the
   least-bad approach I have found.  */
static void
close_unnecessary_fds(void)
{
  DIR *fdir = opendir("/proc/self/fd");
  if (fdir) {
    int dfd = dirfd(fdir);
    struct dirent dent, *dent_out;
    int fd;

    for (;;) {
      if ((errno = readdir_r(fdir, &dent, &dent_out)) != 0)
        fatal_perror("readdir: /proc/self/fd");
      if (!dent_out)
        break;
      if (!strcmp(dent.d_name, ".") || !strcmp(dent.d_name, ".."))
        continue;

      errno = 0;
      fd = (int)xstrtonum(dent.d_name, 0, INT_MAX,
                          "invalid /proc/self/fd entry");

      if (fd >= 3 && fd != dfd)
        close(fd);
    }

  } else {
    /* Double blech. */
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl))
      fatal_perror("getrlimit");
    for (int fd = 3; fd < (int)rl.rlim_max; fd++)
      close(fd);
  }

  /* It is convenient to set stdin nonblocking at this point, too. */
  int flags = fcntl(0, F_GETFL);
  if (flags == -1 || fcntl(0, F_SETFL, flags | O_NONBLOCK) == -1)
    fatal_perror("fcntl");
}

/* We pass down the environment variables TERM, TZ, LANG, and LC_*.
   We forcibly set PATH to a known-good value.
   All other environment variables are cleared. */

static void
prepare_child_env(char **envp)
{
  strvec nenv;
  memset(&nenv, 0, sizeof(strvec));

  for (size_t i = 0; envp[i]; i++)
    if (startswith(envp[i], "TERM=") ||
        startswith(envp[i], "TZ=") ||
        startswith(envp[i], "LANG=") ||
        startswith(envp[i], "LC_"))
      strvec_append(&nenv, envp[i]);

  strvec_append(&nenv,
                "PATH=/usr/local/bin:/usr/bin:/bin:"
                "/usr/local/sbin:/usr/sbin:/sbin");
  strvec_append(&nenv, 0);
  qsort(nenv.vec, nenv.used - 1, sizeof(char *), compar_str);
  child_env = nenv.vec;
}

/* http://pubs.opengroup.org/onlinepubs/9699919799/functions/sigtimedwait.html
   (APPLICATION USAGE): "Note that in order to ensure that generated
   signals are queued and signal values passed to sigqueue() are
   available in si_value, applications which use sigwaitinfo() or
   sigtimedwait() need to set the SA_SIGINFO flag for each signal in
   the set (see Signal Concepts). This means setting each signal to be
   handled by a three-argument signal-catching function, even if the
   handler will never be called."

   I'm not clear on whether this applies to signalfd as well, so this is
   partially a defensive action.  */
static void
dummy_signal_handler(int UNUSED(sig),
                     siginfo_t *UNUSED(info),
                     void *UNUSED(ctxt))
{
}

/* If we are so unfortunate as to take a fatal CPU exception we should
   at least try to kill the openvpn client on the way out. */
static char fatal_signal_stack[SIGSTKSZ];
static void
fatal_signal_handler(int sig)
{
  if (controller_cleanups && cl_ovpn_pid)
    kill(cl_ovpn_pid, SIGTERM);
  raise(sig);
}

static int
prepare_signals(void)
{
  sigset_t parent_sigmask;

  /* in the parent, basically all signals are blocked and handled via
     signalfd */
  sigfillset(&parent_sigmask);

  /* signals that cannot be caught */
  sigdelset(&parent_sigmask, SIGKILL);
  sigdelset(&parent_sigmask, SIGSTOP);

  /* signals indicating a fatal CPU exception or user abort */
  sigdelset(&parent_sigmask, SIGABRT);
  sigdelset(&parent_sigmask, SIGBUS);
  sigdelset(&parent_sigmask, SIGFPE);
  sigdelset(&parent_sigmask, SIGILL);
  sigdelset(&parent_sigmask, SIGSEGV);
  sigdelset(&parent_sigmask, SIGQUIT);

  /* save current signal mask for child */
  if (sigprocmask(SIG_SETMASK, &parent_sigmask, &child_sigmask))
    fatal_perror("sigprocmask");

  /* The only signal whose details we inspect is SIGCHLD, so we can
     get away with just setting the handler for that one.  */
  struct sigaction sa;
  memset(&sa, 0, sizeof(struct sigaction));
  sa.sa_sigaction = dummy_signal_handler;
  sa.sa_flags = SA_RESTART|SA_SIGINFO|SA_NOCLDSTOP;
  if (sigaction(SIGCHLD, &sa, 0))
    fatal_perror("sigaction");

  int sfd = signalfd(-1, &parent_sigmask, SFD_NONBLOCK|SFD_CLOEXEC);
  if (sfd == -1)
    fatal_perror("signalfd");

  /* for great defensiveness, CPU exceptions are fielded with regular
     signal handlers on an alternate stack, just so we can make sure
     to throw a SIGTERM at the openvpn client on the way out, if it's
     running. */
  stack_t ss;
  ss.ss_sp = fatal_signal_stack;
  ss.ss_flags = 0;
  ss.ss_size = SIGSTKSZ;
  if (sigaltstack(&ss, 0))
    fatal_perror("sigaltstack");

  sa.sa_handler = fatal_signal_handler;
  /* sa_flags is an int, but on some systems SA_RESETHAND is 0x8000000
     which is unsigned. */
  sa.sa_flags = (int)(SA_ONSTACK|SA_RESETHAND|SA_NODEFER);
  if (sigaction(SIGABRT, &sa, 0)) fatal_perror("sigaction");
  if (sigaction(SIGBUS,  &sa, 0)) fatal_perror("sigaction");
  if (sigaction(SIGFPE,  &sa, 0)) fatal_perror("sigaction");
  if (sigaction(SIGILL,  &sa, 0)) fatal_perror("sigaction");
  if (sigaction(SIGSEGV, &sa, 0)) fatal_perror("sigaction");
  if (sigaction(SIGQUIT, &sa, 0)) fatal_perror("sigaction");

  return sfd;
}

static pid_t
launch_ovpn(const char *namespace, const char *cfgfile, char **ovpn_args)
{
  pid_t mypid = getpid();
  const char *up_script = xasprintf
    ("%s --as-up-script %s %d", full_progname, namespace, mypid);
  const char *down_script = xasprintf
    ("%s --as-down-script %s", full_progname, namespace);

  strvec oargv;
  memset(&oargv, 0, sizeof(strvec));

  strvec_append(&oargv, "openvpn");
  strvec_append(&oargv, "--config");
  strvec_append(&oargv, cfgfile);
  strvec_append(&oargv, "--ifconfig-noexec");
  strvec_append(&oargv, "--route-noexec");
  strvec_append(&oargv, "--script-security");
  strvec_append(&oargv, "2");
  strvec_append(&oargv, "--up");
  strvec_append(&oargv, up_script);
  strvec_append(&oargv, "--down");
  strvec_append(&oargv, down_script);

  for (int i = 0; ovpn_args[i]; i++)
    strvec_append(&oargv, ovpn_args[i]);
  strvec_append(&oargv, 0);

  return xspawnvp(oargv.vec);
}

static bool
controller_process_signals(int sigfd, pid_t ovpn_pid)
{
  struct signalfd_siginfo ssi;
  pid_t pid;

  for (;;) {
    ssize_t n = read(sigfd, &ssi, sizeof ssi);
    if (n == 0 || (n == -1 && errno == EAGAIN))
      break;
    if (n != sizeof ssi)
      fatal_perror("read");

    switch (ssi.ssi_signo) {
    case SIGUSR1:
      /* Up-script reports completion.  We pass this onward by
         writing a sentinel value to stdout and then closing it. */
      if (!already_closed_stdout) {
        if (write(1, "READY\n", 6) != 6)
          fatal_perror("write");
        if (close(1))
          fatal_perror("close");
        /* for great defensiveness */
        if (open("/dev/null", O_WRONLY) != 1)
          fatal_perror("/dev/null");
        already_closed_stdout = true;
      }
      break;

    case SIGCHLD:
      /* All the necessary information is in 'ssi', but we still need to
         call 'waitpid' to reap the zombie. */
      pid = (pid_t)ssi.ssi_pid;
      if (waitpid(pid, 0, WNOHANG) != pid)
        fatal_perror("waitpid");

      if (pid != ovpn_pid) {
        fprintf(stderr,
                "%s: warning: unexpected SIGCHLD for pid %d "
                "(code=%d status=%d)\n",
                progname, pid, ssi.ssi_code, ssi.ssi_status);
        break;
      }

      /* openvpn has exited, it is no longer necessary to kill it on
         the way out. */
      cl_ovpn_pid = 0;
      if ((ssi.ssi_code == CLD_EXITED && ssi.ssi_status == 0) ||
          (ssi.ssi_code == CLD_KILLED && ssi.ssi_status == SIGTERM))
        /* We're done. */
        return true;

      if (ssi.ssi_code == CLD_EXITED)
        fatal_printf("openvpn: unsuccessful exit code %d",
                     ssi.ssi_status);

      if (ssi.ssi_code == CLD_KILLED || ssi.ssi_code == CLD_DUMPED)
        fatal_printf("openvpn: %s%s",
                     strsignal(ssi.ssi_status),
                     ssi.ssi_code == CLD_DUMPED ? " (core dumped)" : "");

      /* Other values of ssi.ssi_code should not occur. */
      fatal_printf("Unexpected child status change: si_code=%d si_status=%d",
                   ssi.ssi_code, ssi.ssi_status);

    case SIGTSTP:
    case SIGTTIN:
    case SIGTTOU:
      /* blockable signal that would normally stop the process */
      kill(ovpn_pid, SIGTSTP);
      raise(SIGSTOP);
      kill(ovpn_pid, SIGCONT);
      break;

    case SIGURG:
    case SIGWINCH:
      /* signal that would normally be ignored */
      break;

    default:
      /* signal that would normally terminate the process; kill openvpn
         and wait for it to terminate */
      kill(ovpn_pid, SIGTERM);
      break;
    }
  }
  return false;
}

static bool
controller_process_stdin(int fd, short events)
{
  bool closed = false;
  if (events & POLLIN) {
    char scratch[4096];
    ssize_t n;
    do
      n = read(fd, scratch, 4096);
    while (n > 0);
    if (n == 0)
      closed = true;
    if (n < 0 && errno != EAGAIN)
      fatal_perror("read");
  }
  if (events & POLLHUP)
    closed = true;

  return closed;
}

static void
controller_idle_loop(int sigfd, pid_t ovpn_pid)
{
  /* We are waiting for either a signal, or stdin to be closed from
     the far end.  */
  struct pollfd pfds[2];
  pfds[0].fd = sigfd;
  pfds[0].events = POLLIN;
  pfds[1].fd = 0;
  pfds[1].events = POLLIN;

  /* stdin is taken out of the list if it's closed before openvpn
     terminates. */
  nfds_t nfds = 2;

  for (;;) {
    if (poll(pfds, nfds, -1) == -1)
      fatal_perror("poll");

    if (pfds[0].revents)
      if (controller_process_signals(sigfd, ovpn_pid))
        break;

    if (nfds > 1 && pfds[1].revents) {
      if (controller_process_stdin(0, pfds[1].revents)) {
        /* stdin was closed; time to shut down. */
        kill(ovpn_pid, SIGTERM);
        nfds = 1;
      }
    }
  }
}

static void
do_controller(int argc, char **argv, char **envp)
{
  /* argument validation */
  if (argc < 3)
    usage();

  const char *namespace = argv[1];
  if (strlen(namespace) != strspn(namespace, "_0123456789"
                                  "abcdefghijklmnopqrstuvwxyz"
                                  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
    fatal_printf("namespace name '%s' should consist solely of letters, "
                 "digits, and underscores", argv[1]);

  const char *cfgfile = argv[2];
  int testfd = open(cfgfile, O_RDONLY);
  if (testfd == -1 || close(testfd))
    fatal_perror(cfgfile);

  /* parent-side preparation */
  is_controller = true;
  controller_cleanups = true;

  close_unnecessary_fds();
  prepare_child_env(envp);
  int sigfd = prepare_signals();

  pid_t ovpn_pid = launch_ovpn(namespace, cfgfile, argv + 3);
  cl_ovpn_pid = ovpn_pid;

  controller_idle_loop(sigfd, ovpn_pid);
}

static NORETURN
usage(void)
{
  printf("usage: %s namespace openvpn-config [openvpn-args...]\n",
         progname);
  exit(2);
}

static NORETURN
cleanup_and_exit(int status)
{
  if (controller_cleanups && cl_ovpn_pid)
    kill(cl_ovpn_pid, SIGTERM);
  exit(status);
}

int
main(int argc, char **argv, char **envp)
{
  progname = strrchr(argv[0], '/');
  if (progname)
    progname++;
  else
    progname = argv[0];

  full_progname = (const char *)getauxval(AT_EXECFN);
  if (!full_progname) {
    full_progname = realpath(argv[0], 0);
    if (!full_progname)
      fatal_eprintf("unable to determine full pathname of executable "
                    "(argv[0]=%s)", argv[0]);
  }

  /* Because of the way we get openvpn to reexecute this program,
     'full_progname' must not contain shell metacharacters. */
  if (strlen(full_progname) != strspn(full_progname,
                                      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                      "abcdefghijklmnopqrstuvwxyz"
                                      "0123456789%+,-./:=@_")) {
      fprintf(stderr, "usage: the absolute pathname of this program,\n  %s\n"
              "may contain only ASCII letters, digits, and 'safe' "
              "punctuation\n",
              full_progname);
      exit(2);
    }

  /* Line-buffer stderr so that any error messages we emit are
     atomically written (all of our messages are exactly one line).  */
  char stderr_buffer[BUFSIZ];
  setvbuf(stderr, stderr_buffer, _IOLBF, BUFSIZ);

  /* This program arranges to be re-executed with special command line
     arguments as the OpenVPN up and down scripts. */
  if (argc > 1 && argv[1][0] == '-') {
    if (!strcmp(argv[1], "--as-up-script"))
      do_up_script(argc, argv, envp);
    else if (!strcmp(argv[1], "--as-up-inside-ns"))
      do_up_inside_ns(argc, argv, envp);
    else if (!strcmp(argv[1], "--as-down-script"))
      do_down_script(argc, argv, envp);
    else
      usage();
  }
  else
    do_controller(argc, argv, envp);

  cleanup_and_exit(0);
}
