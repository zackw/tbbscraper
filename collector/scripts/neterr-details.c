/* Repeat an HTTP query to record the network errors.
 *
 * Copyright © 2017 Zack Weinberg
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * There is NO WARRANTY.
 * Partially based on public-domain example code from the gnutls manual:
 * https://gnutls.org/manual/
 *
 *     neterr-details [--tls] [--alpn=X:Y:Z] HOST PORT
 *
 * attempts to connect to HOST at PORT, and reports details of any
 * network error that may happen.  PORT may be either a number or an
 * /etc/services name.
 *
 * If --tls is given on the command line, a successful TCP connection
 * will cause an attempt at a TLS handshake.  If --alpn is also given,
 * the sequence of colon-separated strings which follows will be sent
 * in an Application-Layer Protocol Negotiation extension.  (At present,
 * no registered ALPN strings contain a colon.)
 *
 * If connection and TLS handshake (when applicable) are successful,
 * this program immediately disconnects again and reports "success" --
 * no application-layer payload is ever transmitted.
 *
 * Output is to stdout and consists of one or more key:value\n strings.
 * The first of these will always be "status:STATUS\n" where STATUS is
 * one of the strings
 *
 *   dns-error
 *   dns-notfound
 *   tcp-error
 *   tcp-refused
 *   tcp-reset
 *   tcp-unreachable
 *   tls-error
 *   tls-cert-invalid
 *   tls-cert-selfsigned
 *   tls-cert-untrusted
 *   timeout
 *   success
 *
 * For dns-notfound, tcp-refused, tcp-reset, tcp-unreachable, timeout,
 * and success, there is no more to say.  For all of the others, the
 * next line will be "detail:DETAILS\n", with DETAILS a human-readable
 * string giving more technical detail.  For tls-cert-*, after DETAILS
 * will follow "cert:CERTIFICATE\n" lines, one for each cert in the
 * chain supplied by the server, where CERTIFICATE is the certificate
 * in raw base64 format (no ------BEGIN/END CERTIFICATE------- and no
 * internal newlines).
 *
 * Will only exit unsuccessfully upon an internal failure within
 * gnutls, in which case there will be an error message on stderr and
 * nothing on stdout.
 *
 * This program should be mostly portable to other Unix-like operating
 * systems, except that it uses GNU argp (which the makefile assumes is
 * available from libc) out of laziness.
 */

#define _GNU_SOURCE 1
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <argp.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <netdb.h>
#include <signal.h>
#include <unistd.h>

#include <gnutls/gnutls.h>
#include <gnutls/x509.h>

#if defined __GNUC__ && __GNUC__ >= 3
#define UNUSED(param) param __attribute__((unused))
#define NORETURN void __attribute__((noreturn))
#else
#define UNUSED(param) param
#define NORETURN void
#endif

static NORETURN
fatal_perror(const char *msg)
{
  perror(msg);
  exit(1);
}

static struct addrinfo *
dns_lookup(const char *host, const char *port)
{
  struct addrinfo *res = 0;
  struct addrinfo hints;
  memset(&hints, 0, sizeof hints);
  hints.ai_flags    = AI_V4MAPPED|AI_ADDRCONFIG;
  hints.ai_family   = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  int err = getaddrinfo(host, port, &hints, &res);
  if (err) {
    /* The getaddrinfo-level behavior upon NXDOMAIN can be any of these.  */
    if (err == EAI_ADDRFAMILY || err == EAI_NODATA || err == EAI_NONAME) {
      puts("status:dns-notfound");
    } else {
      puts("status:dns-error");
      if (err == EAI_SYSTEM) {
        printf("detail:%s\n", strerror(errno));
      } else if (err != 0) {
        printf("detail:%s\n", gai_strerror(err));
      }
    }
    if (res) {
      freeaddrinfo(res);
    }
    return 0;
  }
  else if (!res) {
    /* This can *also* happen for an NXDOMAIN.  */
    puts("status:dns-notfound");
    return 0;
  }

  return res;
}

static volatile sig_atomic_t timeout_expired;
static void
timeout_handler(int UNUSED(n))
{
  timeout_expired = 1;
}

static int
tcp_connect(struct addrinfo *addrlist)
{
  struct addrinfo *ai;
  int sock, err = 0, rv;

  /* The easiest way to implement a connection timeout is with
     good old-fashioned alarm(2).  */
  struct sigaction sa, oALRM, oINT;
  sigemptyset(&sa.sa_mask);
  sa.sa_handler = timeout_handler;
  sa.sa_flags   = 0; /* DO interrupt blocking system calls! */
  if (sigaction(SIGALRM, &sa, &oALRM) || sigaction(SIGINT, &sa, &oINT))
    fatal_perror("sigaction");

  for (ai = addrlist; ai; ai = ai->ai_next) {
    sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (sock == -1) {
      err = errno;
      continue;
    }

    timeout_expired = 0;
    alarm(60);
    do {
      rv = connect(sock, ai->ai_addr, ai->ai_addrlen);
    } while (rv == -1 && errno == EINTR && !timeout_expired);
    if (rv == 0)
      goto success;
    err = errno;
    close(sock);
  }

  alarm(0);
  sigaction(SIGALRM, &oALRM, 0);
  sigaction(SIGINT, &oINT, 0);

  switch (err) {
  case EHOSTUNREACH:
  case ENETUNREACH:  puts("status:tcp-unreachable"); break;
  case ECONNREFUSED: puts("status:tcp-refused");     break;
  case ECONNRESET:   puts("status:tcp-reset");       break;
  case EINTR:        puts("status:timeout");         break;
  default:
    puts("status:tcp-error");
    printf("detail:%s\n", strerror(err));
  }
  return -1;

 success:
  alarm(0);
  sigaction(SIGALRM, &oALRM, 0);
  sigaction(SIGINT, &oINT, 0);
  return sock;
}

static bool
is_selfsigned(const gnutls_datum_t *raw_certs, unsigned int n_certs)
{
  if (n_certs > 1)
    return false;

  bool result = false;
  gnutls_x509_crt_t cert = 0;
  gnutls_datum_t subject = { 0, 0 };
  gnutls_datum_t issuer  = { 0, 0 };

  if (!gnutls_x509_crt_init(&cert) &&
      !gnutls_x509_crt_import(cert, &raw_certs[0], GNUTLS_X509_FMT_DER) &&
      !gnutls_x509_crt_get_raw_dn(cert, &subject) &&
      !gnutls_x509_crt_get_raw_issuer_dn(cert, &issuer)) {

    result = (subject.size == issuer.size &&
              !memcmp(subject.data, issuer.data, subject.size));
  }

  if (subject.data) gnutls_free(subject.data);
  if (issuer.data)  gnutls_free(issuer.data);
  if (cert)         gnutls_free(cert);

  return result;
}

static void
dump_certs(const gnutls_datum_t *certs, unsigned int n_certs)
{
  gnutls_datum_t encoded;
  char *p, *q;
  unsigned int i;

  for (i = 0; i < n_certs; i++) {
    /* The documentation says you can pass NULL as the first argument
       to base64_encode2, but this is not true; it will crash.  */
    gnutls_pem_base64_encode2("", &certs[i], &encoded);

    /* Print the base64'd cert all on one line, with the PEM wrapper
       removed.  */
    p = strchr((char *)encoded.data, '\n');
    if (!p) {
      printf("cert:%s\n", encoded.data);
      continue;
    }
    fputs("cert:", stdout);
    q = p + 1;
    while (*q && strncmp(q, "-----END", sizeof "-----END" - 1)) {
      p = strchr(q, '\n');
      if (!p) {
        fputs(q, stdout);
        break;
      } else {
        printf("%.*s", (int)(p - q), q);
        q = p + 1;
      }
    }
    putchar('\n');
    gnutls_free(encoded.data);
  }
}

static int
tls_handshake(gnutls_session_t session, int sock)
{
  gnutls_transport_set_int(session, sock);
  gnutls_handshake_set_timeout(session, GNUTLS_DEFAULT_HANDSHAKE_TIMEOUT);

  int ret;
  do {
    ret = gnutls_handshake(session);
  } while (ret < 0 && gnutls_error_is_fatal(ret) == 0);

  if (ret >= 0) {
    gnutls_bye(session, GNUTLS_SHUT_RDWR);
    return 0;
  }

  if (ret == GNUTLS_E_CERTIFICATE_VERIFICATION_ERROR) {
    /* We want to distinguish three cases: the certificate is
       _invalid_ if it is for the wrong domain, expired, or has some
       other problem not related to the PKI; it is _selfsigned_ if it
       is self-signed (and has no connection to the standard PKI) but
       is otherwise fine; and it is _untrusted_ if it was signed by a
       root certificate that isn't in the trust store, or has some
       other problem that could indicate it's a forgery.  */

    gnutls_certificate_type_t type;
    unsigned status;
    gnutls_datum_t print;
    const gnutls_datum_t *certs;
    unsigned int n_certs;

    type = gnutls_certificate_type_get(session);
    status = gnutls_session_get_verify_cert_status(session);
    certs = gnutls_certificate_get_peers(session, &n_certs);

    if (status == (GNUTLS_CERT_INVALID|GNUTLS_CERT_SIGNER_NOT_FOUND)) {
      puts(is_selfsigned(certs, n_certs)
           ? "status:tls-selfsigned" : "status:tls-untrusted");

    } else if (status == (status & (GNUTLS_CERT_INVALID |
                                    GNUTLS_CERT_SIGNATURE_FAILURE |
                                    GNUTLS_CERT_SIGNER_NOT_FOUND |
                                    GNUTLS_CERT_SIGNER_NOT_CA |
                                    GNUTLS_CERT_SIGNER_CONSTRAINTS_FAILURE |
                                    GNUTLS_CERT_PURPOSE_MISMATCH |
                                    GNUTLS_CERT_MISSING_OCSP_STATUS))) {
      puts("status:tls-untrusted");

    } else {
      puts("status:tls-invalid");
    }

    ret = gnutls_certificate_verification_status_print(status, type, &print, 0);
    if (ret < 0) {
      printf("detail:problem reporting verification status:%s\n",
             gnutls_strerror(ret));
    } else {
      if (!strncmp((char *)print.data, "The certificate is NOT trusted. ",
                   sizeof "The certificate is NOT trusted. " - 1)) {
        printf("detail:%s [%08x]\n",
               print.data + sizeof "The certificate is NOT trusted. " - 1,
               status);
      } else {
        printf("detail:%s [%08x]\n", (char *)print.data, status);
        }
      gnutls_free(print.data);
    }

    dump_certs(certs, n_certs);
    /* ??? Are we supposed to deallocate the data returned by
       gnutls_certificate_get_peers? It doesn't matter, we're about to
       exit anyway.  */

  } else if (ret == GNUTLS_E_FATAL_ALERT_RECEIVED ||
             ret == GNUTLS_E_WARNING_ALERT_RECEIVED) {
    printf("status:tls-error\ndetail:alert:%s\n",
           gnutls_alert_get_name(gnutls_alert_get(session)));
  } else {
    printf("status:tls-error\ndetail:%s\n", gnutls_strerror(ret));
  }
  return -1;
}

static void
parse_alpn_protos(const char *alpn_protos,
                  gnutls_datum_t **alpn_vec,
                  unsigned int *alpn_n)
{
  unsigned int n = 0;
  unsigned int i;
  const char *p;
  unsigned char *s, *q;
  gnutls_datum_t *vec;

  for (p = alpn_protos; *p; p++) {
    if (*p == ':') {
      n++;
    }
  }
  if (p != alpn_protos) {
    n++;
  }
  if (n == 0) {
    *alpn_vec = 0;
    *alpn_n = 0;
    return;
  }

  vec = malloc(n*sizeof(gnutls_datum_t) + (size_t)(p + 1 - alpn_protos));
  if (!vec) {
    fatal_perror("malloc");
  }
  s = ((unsigned char *)vec) + n*sizeof(gnutls_datum_t);
  p = alpn_protos;
  q = s;
  i = 0;
  for (;;) {
    if (*p == '\0' || *p == ':') {
      *q = '\0';
      vec[i].data = s;
      vec[i].size = (unsigned)(q - s);
      if (*p == '\0') break;
      s = q + 1;
      i++;
    } else {
      *q = (unsigned char)*p;
    }
    p++;
    q++;
  }
  *alpn_vec = vec;
  *alpn_n = n;
}

static void
setup_gnutls(gnutls_session_t *session,
             gnutls_certificate_credentials_t *xcred,
             const char *host,
             const char *alpn_protos)
{
  if (!gnutls_check_version("3.4.6")) {
    fputs("GnuTLS library too old (>=3.4.6 required)\n", stderr);
    exit(1);
  }

  const char *what;
  int res;
  const char *errptr = 0;
#define XGT(fn, args) do {                              \
    what = #fn;                                         \
    if ((res = gnutls_##fn args) < 0) goto fail;        \
  } while (0)

  XGT(global_init, ());
  XGT(certificate_allocate_credentials, (xcred));
  XGT(certificate_set_x509_system_trust, (*xcred));

  XGT(init, (session, GNUTLS_CLIENT));
  XGT(server_name_set, (*session, GNUTLS_NAME_DNS, host, strlen(host)));
  XGT(credentials_set, (*session, GNUTLS_CRD_CERTIFICATE, *xcred));
  gnutls_session_set_verify_cert(*session, host, 0); /* can't fail */

  /* This priority string matches the out-of-the-box ClientHello
     generated by Firefox 52.0.2 (ESR) as closely as possible.  */
  XGT(priority_set_direct,
      (*session,
       "NONE:%COMPAT"
       ":+VERS-TLS1.2:+VERS-TLS1.1:+VERS-TLS1.0:+CTYPE-X.509"
       ":+AES-128-GCM:+CHACHA20-POLY1305:+AES-256-GCM"
       ":+AES-256-CBC:+AES-128-CBC:+3DES-CBC"
       ":+AEAD:+SHA1"
       ":+ECDHE-ECDSA:+ECDHE-RSA:+DHE-RSA:+RSA"
       ":+COMP-NULL"
       ":+CURVE-X25519:+CURVE-SECP256R1:+CURVE-SECP384R1:+CURVE-SECP521R1"
       ":+SIGN-ECDSA-SHA256:+SIGN-ECDSA-SHA384:+SIGN-ECDSA-SHA512"
     //not supported by gnutls 3.5:
     //":+SIGN-RSA-PSS-SHA256:+SIGN-RSA-PSS_SHA384:+SIGN-RSA-PSS-SHA512"
       ":+SIGN-RSA-SHA256:+SIGN-RSA-SHA384:+SIGN-RSA-SHA512"
       ":+SIGN-ECDSA-SHA1:+SIGN-RSA-SHA1"
       , &errptr));

  /* Similarly, 1023-bit DH primes are the minimum in Firefox 52.0.2.
     We don't use SRP in this program so we don't need to set that.  */
  gnutls_dh_set_prime_bits(*session, 1023);

  if (alpn_protos) {
    gnutls_datum_t *protocols;
    unsigned int nprotos;
    parse_alpn_protos(alpn_protos, &protocols, &nprotos);
    if (nprotos) {
      gnutls_alpn_set_protocols(*session, protocols, nprotos, 0);
    }
  }

  return;

 fail:
  fprintf(stderr, "%s: %s\n", what, gnutls_strerror(res));
  if (errptr)
    fprintf(stderr, "%s: at %.20s\n", what, errptr);
  exit(1);
}

static void
teardown_gnutls(gnutls_session_t session,
                gnutls_certificate_credentials_t xcred)
{
  gnutls_deinit(session);
  gnutls_certificate_free_credentials(xcred);
  gnutls_global_deinit();
}

static void
run(const char *host, const char *port, bool do_tls, const char *alpn_protos)
{
  gnutls_certificate_credentials_t xcred = 0;
  gnutls_session_t session = 0;
  if (do_tls)
    setup_gnutls(&session, &xcred, host, alpn_protos);

  struct addrinfo *addrlist = dns_lookup(host, port);
  if (!addrlist)
    return;

  int sock = tcp_connect(addrlist);
  freeaddrinfo(addrlist);
  if (sock < 0)
    return;

  if (!do_tls || !tls_handshake(session, sock))
    puts("status:success");

  close(sock);

  if (do_tls)
    teardown_gnutls(session, xcred);
}

struct parsed_args
{
  const char *host;
  const char *port;
  const char *alpn_protos;
  bool do_tls;
};

static error_t
parse_opt (int key, char *arg, struct argp_state *state)
{
  struct parsed_args *pa = (struct parsed_args *)state->input;
  switch (key) {
  default:
    return ARGP_ERR_UNKNOWN;

  case 't':
    if (pa->do_tls) {
      argp_error(state, "--tls option specified twice");
    }
    pa->do_tls = 1;
    break;

  case 'a':
    if (pa->alpn_protos) {
      argp_error(state, "--alpn option specified twice");
    }
    if (!arg) {
      argp_error(state, "--alpn option requires an argument");
    }
    pa->alpn_protos = arg;
    break;

  case ARGP_KEY_ARG:
    switch (state->arg_num) {
    case 0:
      pa->host = arg;
      break;

    case 1: {
      /* To validate ARG, we make a dummy call to getaddrinfo in "passive"
         mode.  */
      struct addrinfo hints, *addrs;
      int err;

      memset(&hints, 0, sizeof hints);
      hints.ai_flags = AI_PASSIVE;
      hints.ai_family = AF_UNSPEC;
      hints.ai_socktype = SOCK_STREAM;
      err = getaddrinfo(0, arg, &hints, &addrs);
      if (err) {
        if (err == EAI_SERVICE) {
          argp_error(state, "'%s' is not a valid TCP port or service name",
                     arg);
        } else if (err == EAI_SYSTEM) {
          argp_failure(state, 1, 0, "error while validating port '%s': %s",
                       arg, strerror(errno));
        } else {
          argp_failure(state, 1, 0, "error while validating port '%s': %s",
                       arg, gai_strerror(err));
        }
      }
      freeaddrinfo(addrs);
      pa->port = arg;
      break;
    }

    default:
      argp_error(state, "too many arguments");
    }
    break;

  case ARGP_KEY_END:
    if (!pa->host) {
      argp_error(state, "missing HOST argument");
    }
    if (!pa->port) {
      argp_error(state, "missing PORT argument");
    }
    if (pa->alpn_protos && !pa->do_tls) {
      argp_error(state, "--alpn only makes sense with --tls");
    }
    break;
  }
  return 0;
}

int
main(int argc, char **argv)
{
  static const struct argp_option options[] = {
    { "tls", 't', 0, 0, "Perform a TLS handshake after connection.", 0 },
    { "alpn", 'a', "PROTOCOLS", 0,
      "Include an ALPN extension in the ClientHello, specifying PROTOCOLS. "
      "Only valid with --tls.", 1 },
    { 0, 0, 0, 0, 0, 0 }
  };
  static const struct argp argp = {
    options, parse_opt, "HOST PORT",
    "Attempt to connect to HOST at PORT, and report details of any "
    "network error that may happen.",
    0, 0, 0
  };

  struct parsed_args pa = { 0, 0, 0, false };
  argp_parse(&argp, argc, argv, 0, 0, &pa);
  run(pa.host, pa.port, pa.do_tls, pa.alpn_protos);
  return 0;
}
