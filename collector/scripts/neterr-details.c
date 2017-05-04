/* Repeat an HTTP query to record the network errors.
 *
 * Copyright © 2017 Zack Weinberg
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * There is NO WARRANTY.
 *
 *     neterr-details URL
 *
 * attempts to connect to the server for URL (using curl) and reports
 * details of any network error that may happen.  If the connection and TLS
 * handshake (when applicable) are successful, it immediately disconnects
 * again and reports "success" -- no application-layer payload is ever
 * transmitted.
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
 * Will only exit unsuccessfully when initializing curl fails, or in case
 * of an internal-to-curl failure, in which case there will be an error
 * message on stderr and nothing on stdout.
 *
 * It is assumed that you do not need to set CURLOPT_CAINFO nor CURLOPT_CAPATH
 * for certificate validation to work.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CURL_NO_OLDIES 1
#include <curl/curl.h>

#ifndef CURL_GLOBAL_ACK_EINTR
#define CURL_GLOBAL_ACK_EINTR 0
#endif

static void
dump_errbuf(CURLcode res, const char *errbuf, FILE *stream)
{
  size_t len = strlen(errbuf);
  if (len > 0) {
    if (errbuf[len-1] == '\n')
      ((char *)errbuf)[len-1] = '\0';
    fputs(errbuf, stream);
  } else {
    fputs(curl_easy_strerror(res), stream);
  }
  fprintf(stream, " (%d)\n", res);
}

static void
detail_dump_errbuf(CURLcode res, const char *errbuf)
{
  fputs("detail:", stdout);
  dump_errbuf(res, errbuf, stdout);
}

static void
cert_dump_one(const char *cert)
{
  fputs("cert:", stdout);
  const char *p, *q;
  /* The first line is always "Cert:-----BEGIN CERTIFICATE-----\n".
     Skip this.  */
  p = strchr(cert, '\n');
  if (!p) {
    puts("<malformed>");
    return;
  }
  p++;
  while (*p && strcmp(p, "-----END CERTIFICATE-----\n")) {
    q = strchr(p, '\n');
    if (!q) {
      puts("<unexpected EOF>");
      return;
    }
    fwrite(p, 1, q - p, stdout);
    p = q + 1;
  }
  putchar('\n');
}

static void
cert_dump_chain(CURL *curl, const char *errbuf)
{
  struct curl_certinfo *chain = 0;
  CURLcode res;
  if ((res = curl_easy_getinfo(curl, CURLINFO_CERTINFO, &chain)) != 0) {
    fputs("error: getinfo(CERTINFO): ", stderr);
    dump_errbuf(res, errbuf, stderr);
    exit(1);
  }
  if (chain && chain->num_of_certs) {
    for (int i = 0; i < chain->num_of_certs; i++) {
      struct curl_slist *slist;
      for (slist = chain->certinfo[i]; slist; slist = slist->next)
        if (!strncmp(slist->data, "Cert:", sizeof "Cert:" - 1))
          cert_dump_one(slist->data);
    }
  }
}

static int
status_dns_error(CURLcode res, const char *errbuf)
{
  puts("status:dns-error");
  return 1;
}

static int
status_tcp_error(CURL *curl, const char *errbuf)
{
  CURLcode res;
  long err;
  if ((res = curl_easy_getinfo(curl, CURLINFO_OS_ERRNO, &err)) != 0) {
    fputs("error: getinfo(OS_ERRNO): ", stderr);
    dump_errbuf(res, errbuf, stderr);
    exit(1);
  }
  switch (err) {
  case ECONNREFUSED: puts("status:tcp-refused");     return 0;
  case ECONNRESET:   puts("status:tcp-reset");       return 0;
  case EHOSTUNREACH:
  case ENETUNREACH:  puts("status:tcp-unreachable"); return 0;
  default:
    puts("status:tcp-error");
    return 1;
  }
}

static int
status_tls_error(CURLcode res, const char *errbuf)
{
  puts("status:tls-error");
  return 2;
}

static void
report_status(CURL *curl, CURLcode res, const char *errbuf)
{
  switch (res) {
  case CURLE_OK:
    puts("status:success\n");
    return;

  case CURLE_UNSUPPORTED_PROTOCOL:
  case CURLE_URL_MALFORMAT:
  case CURLE_LDAP_INVALID_URL:
    puts("status:invalid-url");
    detail_dump_errbuf(res, errbuf);
    return;

  case CURLE_COULDNT_RESOLVE_PROXY:
  case CURLE_COULDNT_RESOLVE_HOST:
  case CURLE_FTP_CANT_GET_HOST:
    if (status_dns_error(res, errbuf))
      detail_dump_errbuf(res, errbuf);
    return;

  case CURLE_COULDNT_CONNECT:
  case CURLE_INTERFACE_FAILED:
    if (status_tcp_error(curl, errbuf))
      detail_dump_errbuf(res, errbuf);
    return;

  case CURLE_WEIRD_SERVER_REPLY:
  case CURLE_REMOTE_ACCESS_DENIED:
  case CURLE_FTP_ACCEPT_FAILED:
  case CURLE_FTP_WEIRD_PASS_REPLY:
  case CURLE_FTP_WEIRD_PASV_REPLY:
  case CURLE_FTP_WEIRD_227_FORMAT:
  case CURLE_HTTP2:
  case CURLE_FTP_COULDNT_SET_TYPE:
  case CURLE_PARTIAL_FILE:
  case CURLE_FTP_COULDNT_RETR_FILE:
  case CURLE_QUOTE_ERROR:
  case CURLE_HTTP_RETURNED_ERROR:
  case CURLE_UPLOAD_FAILED:
  case CURLE_FTP_PORT_FAILED:
  case CURLE_FTP_COULDNT_USE_REST:
  case CURLE_RANGE_ERROR:
  case CURLE_HTTP_POST_ERROR:
  case CURLE_LDAP_CANNOT_BIND:
  case CURLE_LDAP_SEARCH_FAILED:
  case CURLE_TOO_MANY_REDIRECTS:
  case CURLE_TELNET_OPTION_SYNTAX:
  case CURLE_SEND_ERROR:
  case CURLE_RECV_ERROR:
  case CURLE_BAD_CONTENT_ENCODING:
  case CURLE_USE_SSL_FAILED:
  case CURLE_LOGIN_DENIED:
  case CURLE_TFTP_NOTFOUND:
  case CURLE_TFTP_PERM:
  case CURLE_REMOTE_DISK_FULL:
  case CURLE_TFTP_ILLEGAL:
  case CURLE_TFTP_UNKNOWNID:
  case CURLE_REMOTE_FILE_EXISTS:
  case CURLE_TFTP_NOSUCHUSER:
  case CURLE_REMOTE_FILE_NOT_FOUND:
  case CURLE_SSH:
  case CURLE_SSL_SHUTDOWN_FAILED:
  case CURLE_HTTP2_STREAM:
    puts("status:proto-error");
    detail_dump_errbuf(res, errbuf);
    return;

  case CURLE_FTP_ACCEPT_TIMEOUT:
  case CURLE_OPERATION_TIMEDOUT:
    puts("status:timeout");
    return;

  case CURLE_SSL_CONNECT_ERROR:
  case CURLE_PEER_FAILED_VERIFICATION:
  case CURLE_SSL_CERTPROBLEM:
  case CURLE_SSL_CIPHER:
  case CURLE_SSL_CACERT:
  case CURLE_SSL_ISSUER_ERROR:
  case CURLE_SSL_PINNEDPUBKEYNOTMATCH:
  case CURLE_SSL_INVALIDCERTSTATUS:
    {
      int x = status_tls_error(res, errbuf);
      if (x >= 1)
        detail_dump_errbuf(res, errbuf);
      if (x >= 2)
        cert_dump_chain(curl, errbuf);
    }
    return;

  case CURLE_OBSOLETE29:
  case CURLE_OBSOLETE32:
  case CURLE_BAD_DOWNLOAD_RESUME:
  case CURLE_FILE_COULDNT_READ_FILE:
  case CURLE_OBSOLETE40:
  case CURLE_FUNCTION_NOT_FOUND:
  case CURLE_ABORTED_BY_CALLBACK:
  case CURLE_BAD_FUNCTION_ARGUMENT:
  case CURLE_OBSOLETE44:
  case CURLE_OBSOLETE46:
  case CURLE_UNKNOWN_OPTION:
  case CURLE_OBSOLETE50:
  case CURLE_GOT_NOTHING:
  case CURLE_SSL_ENGINE_NOTFOUND:
  case CURLE_SSL_ENGINE_SETFAILED:
  case CURLE_OBSOLETE57:
  case CURLE_FILESIZE_EXCEEDED:
  case CURLE_SEND_FAIL_REWIND:
  case CURLE_SSL_ENGINE_INITFAILED:
  case CURLE_CONV_FAILED:
  case CURLE_CONV_REQD:
  case CURLE_SSL_CACERT_BADFILE:
  case CURLE_AGAIN:
  case CURLE_SSL_CRL_BADFILE:
  case CURLE_FTP_PRET_FAILED:
  case CURLE_RTSP_CSEQ_ERROR:
  case CURLE_RTSP_SESSION_ERROR:
  case CURLE_FTP_BAD_FILE_LIST:
  case CURLE_CHUNK_FAILED:
  case CURLE_NO_CONNECTION_AVAILABLE:
  case CURLE_FAILED_INIT:
  case CURLE_NOT_BUILT_IN:
  case CURLE_OBSOLETE20:
  case CURLE_WRITE_ERROR:
  case CURLE_OBSOLETE24:
  case CURLE_READ_ERROR:
  case CURLE_OUT_OF_MEMORY:
  case CURL_LAST:
    fputs("error: curl_easy_perform: ", stderr);
    dump_errbuf(res, errbuf, stderr);
    exit(1);
  }
}

int
main(int argc, char **argv)
{
  if (argc != 2) {
    fprintf(stderr, "usage: %s URL\n", argv[0]);
    return 2;
  }

  const char *url = argv[1];

  curl_global_init(CURL_GLOBAL_ALL|CURL_GLOBAL_ACK_EINTR);
  CURL *curl = curl_easy_init();
  if (!curl) {
    fputs("error: curl_easy_init failed\n", stderr);
    return 1;
  }

  CURLcode res;
  char errbuf[CURL_ERROR_SIZE];
  const char *what;
  errbuf[0] = '\0';

#define curl_xsetopt(h, k, v) do {                               \
    what = #k;                                                   \
    if ((res = curl_easy_setopt(h, CURLOPT_##k, v))) goto error; \
  } while (0)

  curl_xsetopt(curl, ERRORBUFFER, errbuf);
  curl_xsetopt(curl, URL, url);
  curl_xsetopt(curl, CONNECT_ONLY, 1L);
  curl_xsetopt(curl, CERTINFO, 1L);

  res = curl_easy_perform(curl);
  report_status(curl, res, errbuf);
  curl_easy_cleanup(curl);
  return 0;

 error:
  fprintf(stderr, "error: curl_easy_setopt(%s): ", what);
  dump_errbuf(res, errbuf, stderr);
  return 1;
}
