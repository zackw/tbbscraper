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
 *     neterr-details HOST PORT
 *
 * attempts to connect to HOST at PORT, and reports details of any
 * network error that may happen.  PORT must be numeric.  If PORT is
 * on a hardwired list of TLS-native TCP ports, and TCP connection is
 * successful, also performs a TLS handshake.  If connection and TLS
 * handshake (when applicable) are successful, it immediately
 * disconnects again and reports "success" -- no application-layer
 * payload is ever transmitted.
 *
 * Output is to stdout and consists of one or more key:value\n strings.
 * The first of these will always be "status:STATUS\n" where STATUS is
 * one of the strings
 *
 *   invalid-url
 *   dns-notfound
 *   dns-error
 *   tcp-refused
 *   tcp-reset
 *   tcp-unreachable
 *   tcp-error
 *   proto-error
 *   tls-crypto-too-weak
 *   tls-cert-invalid
 *   tls-cert-selfsigned
 *   tls-cert-untrusted
 *   timeout
 *   success
 *
 * For invalid-url, dns-notfound, tcp-refused, tcp-reset, tcp-unreachable,
 * timeout, and success, there is no more to say.  For all of the others,
 * the next line will be "detail:DETAILS\n", with DETAILS a human-readable
 * string giving more technical detail (this is usually the CURL error
 * string).  For tls-cert-*, after DETAILS will follow "cert:CERTIFICATE\n"
 * lines, one for each cert in the chain supplied by the server, where
 * CERTIFICATE is the certificate in raw base64 format (no ------BEGIN/END
 * CERTIFICATE------- and no internal newlines).
 *
 * Will only exit unsuccessfully upon an internal failure within
 * gnutls, in which case there will be an error message on stderr and
 * nothing on stdout.
 */

#define _GNU_SOURCE 1
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
  hints.ai_flags    = AI_V4MAPPED|AI_ADDRCONFIG|AI_NUMERICSERV;
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

  printf("*** Handshake failed: %s\n", gnutls_strerror(ret));
  if (ret == GNUTLS_E_FATAL_ALERT_RECEIVED ||
      ret == GNUTLS_E_WARNING_ALERT_RECEIVED) {
    printf("*** Alert detail: %s\n",
           gnutls_alert_get_name(gnutls_alert_get(session)));

  } else if (ret == GNUTLS_E_CERTIFICATE_VERIFICATION_ERROR) {
    gnutls_certificate_type_t type;
    unsigned status;
    gnutls_datum_t out;

    /* check certificate verification status */
    type = gnutls_certificate_type_get(session);
    status = gnutls_session_get_verify_cert_status(session);
    ret = gnutls_certificate_verification_status_print(status, type, &out, 0);
    if (ret < 0) {
      printf("*** cannot report verification status: %s\n",
             gnutls_strerror(ret));
    } else {
      printf("*** cert verify output: %s\n", out.data);
      gnutls_free(out.data);
    }
  }
  return -1;
}

static void
setup_gnutls(gnutls_session_t *session,
             gnutls_certificate_credentials_t *xcred,
             const char *host)
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
       ":+AES-128-GCM:+CHACHA20-POLY1305:+AES-256-GCM:+AES-256-CBC:+AES-128-CBC:+3DES-CBC"
       ":+AEAD:+SHA1"
       ":+ECDHE-ECDSA:+ECDHE-RSA:+DHE-RSA:+RSA"
       ":+COMP-NULL"
       ":+CURVE-X25519:+CURVE-SECP256R1:+CURVE-SECP384R1:+CURVE-SECP521R1"
       ":+SIGN-ECDSA-SHA256:+SIGN-ECDSA-SHA384:+SIGN-ECDSA-SHA512"
     //":+SIGN-RSA-PSS-SHA256:+SIGN-RSA-PSS_SHA384:+SIGN-RSA-PSS-SHA512" not supported by gnutls 3.5
       ":+SIGN-RSA-SHA256:+SIGN-RSA-SHA384:+SIGN-RSA-SHA512:+SIGN-ECDSA-SHA1:+SIGN-RSA-SHA1"
       , &errptr));

  /* Similarly, 1023-bit DH primes are the minimum in Firefox 52.0.2.
     We don't use SRP in this program so we don't need to set that.  */
  gnutls_dh_set_prime_bits(*session, 1023);

  /* Match ALPN extension sent by Firefox as well.  */
  static unsigned char pr1[3] = "h2";
  static unsigned char pr2[9] = "http/1.1";
  static gnutls_datum_t protocols[] = { { pr1, 2 }, { pr2, 8 } };
  gnutls_alpn_set_protocols(*session, protocols, 2, 0);

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

static bool
tls_port_p(const char *port)
{
  return !strcmp(port, "443");
}

int
main(int argc, char **argv)
{
  if (argc != 3) {
    fprintf(stderr, "usage: %s HOST PORT\n", argv[0]);
    return 2;
  }

  const char *host = argv[1];
  const char *port = argv[2];

  bool do_tls = tls_port_p(port);

  gnutls_certificate_credentials_t xcred = 0;
  gnutls_session_t session = 0;
  if (do_tls)
    setup_gnutls(&session, &xcred, host);

  struct addrinfo *addrlist = dns_lookup(host, port);
  if (!addrlist)
    return 0;

  int sock = tcp_connect(addrlist);
  freeaddrinfo(addrlist);
  if (sock < 0)
    return 0;

  if (!do_tls || !tls_handshake(session, sock))
    puts("status:success");

  close(sock);

  if (do_tls)
    teardown_gnutls(session, xcred);

  return 0;
}
