/* Establish network namespaces that will use tunnel devices as their
 * default routes.
 *
 * Copyright © 2015 Zack Weinberg
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * There is NO WARRANTY.
 *
 *     tunnel-ns PREFIX N
 *
 * creates N network namespaces, imaginatively named PREFIX_ns0,
 * PREFIX_ns1, ... The loopback device in each namespace is brought
 * up, with the usual address.  /etc/netns directories for each
 * namespace are created.  No other setup is performed.  (The tunnel
 * interfaces are expected to be created on the fly by a program like
 * 'openvpn-netns', which see.  This is because (AFAICT) if you create
 * a persistent tunnel ahead of time, and put its interface side into
 * a namespace, it then becomes impossible for anything to reattach
 * to the device side.)
 *
 * This program expects to be run with both stdin and stdout connected
 * to pipes.  As it creates each namespace, it writes one line to its
 * stdout:
 *
 *   PREFIX_nsX <newline>
 *
 * After all namespaces have been created, stdout is closed.
 *
 * Anything written to stdin is read and discarded.  When stdin is
 * *closed*, however, all of the network namespaces are torn down
 * (killing any processes still in there, if necessary) and the
 * program exits.  This also happens on receipt of any catchable
 * signal whose default action is to terminate the process without
 * a core dump (e.g. SIGTERM, SIGHUP).
 *
 * Errors, if any, will be written to stderr.
 *
 * This program must be installed setuid root.
 *
 * This program makes extensive use of Linux-specific network stack
 * features.  A port to a different OS might well entail a complete
 * rewrite.  Apart from that, C99 and POSIX.1-2001 features are used
 * throughout.  It also requires dirfd, strdup, and strsignal, from
 * POSIX.1-2008; execvpe, pipe2, and vasprintf, from the shared
 * BSD/GNU extension set; and the currently Linux-specific signalfd.
 */

#define _GNU_SOURCE 1
#define _FILE_OFFSET_BITS 64 /* large directory readdir(), large rlimits */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <sys/resource.h>
#include <sys/signalfd.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#if defined __GNUC__ && __GNUC__ >= 4
#define NORETURN void __attribute__((noreturn))
#define PRINTFLIKE __attribute__((format(printf,1,2)))
#else
#define NORETURN void
#define PRINTFLIKE /*nothing*/
#endif

/* Global state. */

static const char *progname;
static const char *const *child_env;
static sigset_t child_sigmask;

/* cleanup needs to know: */
static bool is_child_process;
static size_t n_namespaces;
static const char **namespace_names;
static const char **nsconfdir_names;

/* Error reporting. */

static NORETURN cleanup_and_exit(int);

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

static PRINTFLIKE NORETURN
usage(const char *msg, ...)
{
  fprintf(stderr, "%s: ", progname);
  va_list ap;
  va_start(ap, msg);
  vfprintf(stderr, msg, ap);
  va_end(ap);
  fprintf(stderr, "\nusage: %s prefix n_namespaces\n", progname);
  cleanup_and_exit(2);
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

static long long
xstrtonum_usage(const char *str, long long minval, long long maxval,
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
    usage("%s: '%s': invalid number", msgprefix, str);
  else if (errno)
    usage("%s: '%s': %s", msgprefix, str, strerror(errno));
  else if (rv < minval)
    usage("%s: '%s': too small (minimum %lld)", msgprefix, str, minval);
  else if (rv > maxval)
    usage("%s: '%s': too large (maximum %lld)", msgprefix, str, maxval);

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
   We don't want the scripts acting up because of that.  This is done
   only for child processes because one of the differences is that a
   setuid program can be killed by the invoking (real) UID, which we
   do want to allow.  */
static void
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
  is_child_process = true;

  /* Child-side stdin and stdout redirections. */
  if (child_stdin != 0) {
    if (close(0) && errno != EBADF)
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
    if (close(1) && errno != EBADF)
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

static void
runv_ignore_failure(const char *const *argv)
{
  pid_t pid = xspawnvp(argv);
  int status;
  if (waitpid(pid, &status, 0) != pid)
    fatal_perror("waitpid");
}
#define run_ignore_failure(...) \
  runv_ignore_failure((const char *const []){ __VA_ARGS__, 0 })

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

static void
runv_get_output_pids(pidvec *v, const char *const *argv)
{
  char *buf = runv_get_output(argv);
  pidvec_from_text(v, buf, argv[0]);
}

/* General setup. */

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
        close((int)fd);
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

static int
prepare_signals(void)
{
  sigset_t parent_sigmask;

  /* Receipt of any catchable signal whose default action is to
     terminate the process without a core dump is treated the same as
     stdin being closed.  All these signals are blocked and handled via
     signalfd.  It is easier to define the signal set negatively.  */
  sigfillset(&parent_sigmask);

  /* signals that cannot be caught */
  sigdelset(&parent_sigmask, SIGKILL);
  sigdelset(&parent_sigmask, SIGSTOP);

  /* signals that normally suspend the process */
  sigdelset(&parent_sigmask, SIGTSTP);
  sigdelset(&parent_sigmask, SIGTTIN);
  sigdelset(&parent_sigmask, SIGTTOU);

  /* signals that are normally ignored */
  sigdelset(&parent_sigmask, SIGCHLD);
  sigdelset(&parent_sigmask, SIGURG);
  sigdelset(&parent_sigmask, SIGWINCH);

  /* signals indicating a fatal CPU exception or user abort */
  sigdelset(&parent_sigmask, SIGABRT);
  sigdelset(&parent_sigmask, SIGBUS);
  sigdelset(&parent_sigmask, SIGFPE);
  sigdelset(&parent_sigmask, SIGILL);
  sigdelset(&parent_sigmask, SIGQUIT);
  sigdelset(&parent_sigmask, SIGSEGV);
  sigdelset(&parent_sigmask, SIGSYS);
  sigdelset(&parent_sigmask, SIGTRAP);

  /* save current signal mask for child procs */
  if (sigprocmask(SIG_SETMASK, &parent_sigmask, &child_sigmask))
    fatal_perror("sigprocmask");

  int sfd = signalfd(-1, &parent_sigmask, SFD_NONBLOCK|SFD_CLOEXEC);
  if (sfd == -1)
    fatal_perror("signalfd");

  return sfd;
}

/* Master control. */

static bool
process_signals(int sigfd)
{
  struct signalfd_siginfo ssi;
  bool done = false;

  for (;;) {
    ssize_t n = read(sigfd, &ssi, sizeof ssi);
    if (n == -1 && errno != EAGAIN)
      fatal_perror("read(signalfd)");
    if (n <= 0)
      break;

    done = true;
  }
  return done;
}

static bool
process_stdin(int fd, short events)
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
idle_loop(int sigfd)
{
  /* We are waiting for either a signal, or stdin to be closed from
     the far end.  */
  bool done = false;

  struct pollfd pfds[2];
  pfds[0].fd = sigfd;
  pfds[0].events = POLLIN;
  pfds[1].fd = 0;
  pfds[1].events = POLLIN;

  while (!done) {
    if (poll(pfds, 2, -1) == -1)
      fatal_perror("poll");

    if (pfds[0].revents)
      done |= process_signals(sigfd);

    if (pfds[1].revents)
      done |= process_stdin(0, pfds[1].revents);
  }
}

static void
create_namespaces(const char *prefix, size_t n)
{
  namespace_names = xreallocarray(0, n+1, sizeof(const char *));
  nsconfdir_names = xreallocarray(0, n+1, sizeof(const char *));
  memset(namespace_names, 0, (n+1) * sizeof(const char *));
  memset(nsconfdir_names, 0, (n+1) * sizeof(const char *));
  n_namespaces = n;

  for (size_t i = 0; i < n; i++) {
    const char *nsp = xasprintf("%s_ns%zd", prefix, i);
    const char *nsc = xasprintf("/etc/netns/%s", nsp);

    if (mkdir(nsc, 0777))
      fatal_perror(nsc);
    nsconfdir_names[i] = nsc;

    run("ip", "netns", "add", nsp);
    namespace_names[i] = nsp;

    /* The loopback interface automatically exists in the namespace,
       with the usual address and an appropriate routing table entry,
       but it is not brought up automatically. */
    run("ip", "netns", "exec", nsp,
        "ip", "link", "set", "dev", "lo", "up");

    puts(nsp);
    fflush(stdout);
  }
  fclose(stdout);
}

static NORETURN
cleanup_and_exit(int status)
{
  if (!is_child_process && n_namespaces > 0) {
    pidvec to_kill;
    memset(&to_kill, 0, sizeof(pidvec));

    for (size_t i = 0; i < n_namespaces; i++) {
      const char *nsp = namespace_names[i];
      const char *nsc = nsconfdir_names[i];

      if (nsp) {
        const char *ipcmd[] = { "ip", "netns", "pids", nsp, 0 };
        runv_get_output_pids(&to_kill, ipcmd);
        if (to_kill.used) {
          pidvec_kill(&to_kill, SIGTERM);
          pidvec_clear(&to_kill);

          sleep(5);
          runv_get_output_pids(&to_kill, ipcmd);
          pidvec_kill(&to_kill, SIGKILL);
          pidvec_clear(&to_kill);
        }

        run_ignore_failure("ip", "netns", "exec", nsp,
                           "ip", "link", "set", "dev", "lo", "down");
        run_ignore_failure("ip", "netns", "del", nsp);
      }

      if (nsc)
        run_ignore_failure("rm", "-rf", nsc);

      if (!nsp || !nsc)
        break;
    }
  }
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

  /* Line-buffer stderr so that any error messages we emit are atomically
     written (all non-usage error messages are exactly one line).  */
  char stderr_buffer[BUFSIZ];
  setvbuf(stderr, stderr_buffer, _IOLBF, BUFSIZ);

  if (argc != 3)
    usage("wrong number of command line arguments");

  const char *prefix = argv[1];
  if (strlen(prefix) != strspn(prefix,
                               "0123456789"
                               "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                               "abcdefghijklmnopqrstuvwxyz"
                               "_"))
    usage("prefix must be only ASCII letters, digits, and underscores");

  size_t nnsp = (size_t)xstrtonum_usage(argv[2], 0, 1024,
                                        "number of namespaces");

  close_unnecessary_fds();

  int sigfd = prepare_signals();
  prepare_child_env(envp);
  create_namespaces(prefix, nnsp);
  idle_loop(sigfd);
  cleanup_and_exit(0);
}
