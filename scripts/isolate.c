/* Wrapper for invoking programs under an isolated uid.
 *
 * Copyright © 2014 Zack Weinberg
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * There is NO WARRANTY.
 *
 * This program runs a command with arguments in a weakly isolated
 * environment.  Specifically, the command is run under its own user
 * and group ID, in a just-created, (almost) empty home directory, in
 * its own background process group.  stdin, stdout, and stderr are
 * inherited from the parent, and all other file descriptors are
 * closed.  HOME, USER, PWD, LOGNAME, TMPDIR, and SHELL are set
 * appropriately; PATH, TZ, TERM, LANG, and LC_* are preserved; all
 * other environment variables are cleared.  CPU and total memory
 * resource limits are applied, and the command will be killed after a
 * ten-minute wall-clock timeout.  After the command exits, its home
 * directory is erased.
 *
 * This is not intended as a replacement for containers!
 * The command can still access the entire filesystem and all other
 * shared resources.  There is no attempt to set extended credentials
 * of any kind, or apply PAM session settings, or anything like that.
 */

/* Compile-time configuration parameters.  May be overridden with -D.
 *
 * This program is to be installed setuid root.  The directory
 * ISOLATE_HOME (default /home/isolated) must exist, be owned by root,
 * and not be used for any other purpose.  The userid range
 * ISOLATE_LOW_UID (default 2000) through ISOLATE_HIGH_UID (default
 * 2999) must not conflict with any existing user or group ID.
 *
 * If you put these uids in /etc/passwd and /etc/group, the username,
 * group membership and shell specified there (but *not* the homedir)
 * will be honored; otherwise, the process will be given a primary GID
 * with the same numeric value as its UID, no supplementary groups,
 * USER and LOGNAME will be set to "iso-NNNN" where NNNN is the
 * decimal UID, and SHELL will be set to "/bin/sh".
 *
 * The other ISOLATE_* parameters control resource limits.
 */
#ifndef ISOLATE_HOME
#define ISOLATE_HOME "/home/isolated"
#endif
#ifndef ISOLATE_LOW_UID
#define ISOLATE_LOW_UID 2000
#endif
#ifndef ISOLATE_HIGH_UID
#define ISOLATE_HIGH_UID 3000
#endif
#ifndef ISOLATE_RLIMIT_CPU
#define ISOLATE_RLIMIT_CPU 60 /* one minute */
#endif
#ifndef ISOLATE_RLIMIT_WALL
#define ISOLATE_RLIMIT_WALL 600 /* ten minutes */
#endif
#ifndef ISOLATE_RLIMIT_MEM
#define ISOLATE_RLIMIT_MEM (1L<<30) /* one gigabyte */
#endif
#ifndef ISOLATE_RLIMIT_CORE
#define ISOLATE_RLIMIT_CORE 0 /* no core dumps */
#endif
#ifndef ISOLATE_UMASK
#define ISOLATE_UMASK 077 /* umask for command */
#endif

/* This program has only been tested on Linux.  C99 and POSIX.1-2001
   features are used throughout.  It also requires dirfd, lchown, and
   strdup from POSIX.1-2008; and execvpe, initgroups, vasprintf, and
   NSIG from the shared BSD/GNU extension set.

   It should not be difficult to port this program to any modern *BSD,
   but it may well be impractical to port it to anything older. */
#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64 /* large directory readdir(), large rlimits */

#include <stdbool.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <limits.h>
#include <pwd.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
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

/* Timespec utilities. */

static struct timespec
timespec_minus(struct timespec end, struct timespec start)
{
  struct timespec temp;
  if (end.tv_nsec - start.tv_nsec < 0) {
    temp.tv_sec = end.tv_sec - start.tv_sec - 1;
    temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
  } else {
    temp.tv_sec = end.tv_sec - start.tv_sec;
    temp.tv_nsec = end.tv_nsec - start.tv_nsec;
  }
  return temp;
}

static struct timespec
timespec_clamp(struct timespec t)
{
  if (t.tv_nsec < 0) {
    t.tv_sec -= 1;
    t.tv_nsec += 1000000000;
  }
  if (t.tv_sec < 0) {
    t.tv_sec = 0;
    t.tv_nsec = 0;
  }
  return t;
}

/* Error reporting. */

static const char *progname;

static NORETURN
fatal(const char *msg)
{
  fprintf(stderr, "%s: %s\n", progname, msg);
  exit(1);
}

static NORETURN
fatal_perror(const char *msg)
{
  fprintf(stderr, "%s: %s: %s\n", progname, msg, strerror(errno));
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
  exit(1);
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
  exit(1);
}

static void *
xmalloc(size_t n)
{
  void *rv = malloc(n);
  if (!n)
    fatal_perror("malloc");
  return rv;
}

static char *
xstrdup(const char *s)
{
  char *rv = strdup(s);
  if (!rv)
    fatal_perror("strdup");
  return rv;
}

static char * PRINTFLIKE
xasprintf(const char *fmt, ...)
{
  char *rv;
  va_list ap;
  va_start(ap, fmt);
  if (vasprintf(&rv, fmt, ap) == -1)
    fatal_perror("asprintf");
  return rv;
}

/* Cleanup of the isolation home directory is queued via atexit(), so that
   we don't have to have code to do it in every fatal_* path.  Note that
   it is not safe to call exit() inside an atexit() handler, so we must
   avoid fatal_* in this function.  */

static const char *homedir;
static pid_t child_pgrp;
static void
cleanups(void)
{
  if (child_pgrp)
    killpg(child_pgrp, SIGKILL);

  if (homedir) {
    const char *rm_args[] = {
      "rm", "-rf", homedir, 0
    };
    const char *noenv[] = { 0 };
    pid_t child;
    int status;

    fflush(0);
    child = fork();
    if (child == -1) {
      fprintf(stderr, "%s: fork (rm): %s\n", progname, strerror(errno));
      return;
    }
    if (child == 0) {
      execvpe("rm", (char * const *)rm_args, (char * const *)noenv);
      fprintf(stderr, "%s: execvpe (rm): %s\n", progname, strerror(errno));
      fflush(0);
      _exit(127);
    }

    if (waitpid(child, &status, 0) != child) {
      fprintf(stderr, "%s: waitpid (rm): %s\n", progname, strerror(errno));
      return;
    }

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
      fprintf(stderr, "%s: rm -rf %s: unsuccessful - status %04x",
              progname, homedir, status);
  }
}

/* Child process state. */

typedef struct child_state
{
  const char *homedir;
  const char *logname;
  const char *shell;
  const char **argv;
  const char **envp;
  uid_t uid;
  gid_t gid;
  sigset_t sigmask;
} child_state;

/* http://pubs.opengroup.org/onlinepubs/9699919799/functions/sigtimedwait.html
   (APPLICATION USAGE): "Note that in order to ensure that generated
   signals are queued and signal values passed to sigqueue() are
   available in si_value, applications which use sigwaitinfo() or
   sigtimedwait() need to set the SA_SIGINFO flag for each signal in
   the set (see Signal Concepts). This means setting each signal to be
   handled by a three-argument signal-catching function, even if the
   handler will never be called."  */
static void
dummy_signal_handler(int UNUSED(sig),
                     siginfo_t *UNUSED(info),
                     void *UNUSED(ctxt))
{
}

static void
prepare_signals(child_state *cs, sigset_t *parent_sigmask)
{
  /* in the parent, basically all signals are blocked and handled via
     sigtimedwait */
  sigfillset(parent_sigmask);

  /* signals that cannot be caught */
  sigdelset(parent_sigmask, SIGKILL);
  sigdelset(parent_sigmask, SIGSTOP);

  /* signals indicating a fatal CPU exception or user abort */
  sigdelset(parent_sigmask, SIGABRT);
  sigdelset(parent_sigmask, SIGBUS);
  sigdelset(parent_sigmask, SIGFPE);
  sigdelset(parent_sigmask, SIGILL);
  sigdelset(parent_sigmask, SIGSEGV);
  sigdelset(parent_sigmask, SIGQUIT);

  /* save current signal mask for child */
  if (sigprocmask(SIG_SETMASK, parent_sigmask, &cs->sigmask))
    fatal_perror("sigprocmask");

  /* The only signal whose siginfo_t we inspect is SIGCHLD, so we can
     get away with just setting the handler for that one.  */
  struct sigaction sa;
  memset(&sa, 0, sizeof(struct sigaction));
  sa.sa_sigaction = dummy_signal_handler;
  sa.sa_flags = SA_RESTART|SA_SIGINFO|SA_NOCLDSTOP;
  if (sigaction(SIGCHLD, &sa, 0))
    fatal_perror("sigaction");
}

/* Infuriatingly, Linux refuses to adopt closefrom(). This is the
   least-bad approach I have found.  */
static void
prepare_fds(void)
{
  DIR *fdir = opendir("/proc/self/fd");
  if (fdir) {
    int dfd = dirfd(fdir);
    struct dirent dent, *dent_out;
    unsigned long fd;
    char *endp;

    for (;;) {
      if ((errno = readdir_r(fdir, &dent, &dent_out)) != 0)
        fatal_perror("readdir: /proc/self/fd");
      if (!dent_out)
        break;
      if (!strcmp(dent.d_name, ".") || !strcmp(dent.d_name, ".."))
        continue;

      errno = 0;
      fd = strtoul(dent.d_name, &endp, 10);
      if (endp == dent.d_name || *endp || errno || fd > (unsigned long)INT_MAX)
        fatal_printf("/proc/self/fd: bogus entry: '%s'", dent.d_name);

      if (fd >= 3 && fd != (unsigned long)dfd)
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
}

static void
prepare_homedir(child_state *cs)
{
  uid_t u;
  char *h;

  for (u = ISOLATE_LOW_UID; u <= ISOLATE_HIGH_UID; u++) {
    h = xasprintf("%s/%u", ISOLATE_HOME, u);
    if (!mkdir(h, 0700))
      goto created;

    if (errno != EEXIST)
      fatal_eprintf("mkdir: %s", h);
    free(h);
  }
  fatal("all isolation uids are in use");

 created:
  homedir = cs->homedir = h;
  cs->uid = u;

  /* getpwuid_r is too complicated to bother with. */
  struct passwd *pw = getpwuid(u);
  if (pw) {
    cs->gid = pw->pw_gid;
    cs->logname = (pw->pw_name && pw->pw_name[0]) ? xstrdup(pw->pw_name)  : 0;
    cs->shell = (pw->pw_shell && pw->pw_shell[0]) ? xstrdup(pw->pw_shell) : 0;
  }
  if (!cs->logname)
    cs->logname = xasprintf("iso-%u", cs->uid);
  if (!cs->shell)
    cs->shell = "/bin/sh";
  if (!cs->gid)
    cs->gid = cs->uid;

  if (lchown(h, cs->uid, cs->gid))
    fatal_eprintf("lchown: %s", h);
}

static inline bool
should_copy_envvar(const char *envvar)
{
#define startswith(x, y) (!strncmp((x), (y), sizeof(y) - 1))
  return (startswith(envvar, "PATH=") ||
          startswith(envvar, "TZ=") ||
          startswith(envvar, "TERM=") ||
          startswith(envvar, "LANG=") ||
          startswith(envvar, "LC_"));
#undef startswith
}

static int
compar_str(const void *a, const void *b)
{
  return strcmp(*(char *const *)a, *(char *const *)b);
}

static void
prepare_environment(child_state *cs, char **envp)
{
  size_t envc;
  size_t i;
  char *tmpdir;

  tmpdir = xasprintf("%s/.tmp", cs->homedir);
  if (mkdir(tmpdir, 0700))
    fatal_eprintf("mkdir: %s", tmpdir);

  envc = 0;
  for (i = 0; envp[i]; i++)
    if (should_copy_envvar(envp[i]))
      envc++;

  /* Six environment variables are force-set:
     HOME PWD TMPDIR USER LOGNAME SHELL
     One more for the terminator. */
  cs->envp = xmalloc((envc + 7) * sizeof(char *));

  envc = 0;
  for (i = 0; envp[i]; i++)
    if (should_copy_envvar(envp[i]))
      cs->envp[envc++] = envp[i];

  cs->envp[envc++] = xasprintf("HOME=%s", cs->homedir);
  cs->envp[envc++] = xasprintf("PWD=%s", cs->homedir);
  cs->envp[envc++] = xasprintf("TMPDIR=%s", tmpdir);
  cs->envp[envc++] = xasprintf("USER=%s", cs->logname);
  cs->envp[envc++] = xasprintf("LOGNAME=%s", cs->logname);
  cs->envp[envc++] = xasprintf("SHELL=%s", cs->shell);
  cs->envp[envc] = 0;

  qsort(cs->envp, envc, sizeof(char *), compar_str);
  free(tmpdir);
}

static NORETURN
run_isolated_child(child_state *cs)
{
  struct rlimit rl;

  /* This code executes on the child side of a fork(), but the parent
     has arranged for it to be safe for us to write to stderr under
     error conditions.  Disable the cleanup handler so it doesn't get
     run twice in case of failure. */
  homedir = 0;

  if (chdir(cs->homedir))
    fatal_eprintf("chdir: %s", cs->homedir);

  /* Reset signal handling. */
  if (sigprocmask(SIG_SETMASK, &cs->sigmask, 0))
    fatal_perror("sigprocmask");

  /* Apply resource limits. */
  rl.rlim_cur = ISOLATE_RLIMIT_CPU;
  rl.rlim_max = ISOLATE_RLIMIT_CPU;
  if (setrlimit(RLIMIT_CPU, &rl))
    fatal_perror("setrlimit(RLIMIT_CPU)");

  rl.rlim_cur = ISOLATE_RLIMIT_MEM;
  rl.rlim_max = ISOLATE_RLIMIT_MEM;
  if (setrlimit(RLIMIT_AS, &rl))
    fatal_perror("setrlimit(RLIMIT_AS)");
  if (setrlimit(RLIMIT_DATA, &rl))
    fatal_perror("setrlimit(RLIMIT_DATA)");

  rl.rlim_cur = ISOLATE_RLIMIT_CORE;
  rl.rlim_max = ISOLATE_RLIMIT_CORE;
  if (setrlimit(RLIMIT_CORE, &rl))
    fatal_perror("setrlimit(RLIMIT_CORE)");

  /* Wall-clock timeout is applied by the parent. */

  /* Drop privileges. */
  if (initgroups(cs->logname, cs->gid))
    fatal_perror("initgroups");
  if (setgid(cs->gid))
    fatal_perror("setgid");
  if (setuid(cs->uid))
    fatal_perror("setuid");

  /* Establish a new, background process group. */
  if (setpgid(0, 0))
    fatal_perror("setpgid");

  /* umask() cannot fail. */
  umask(ISOLATE_UMASK);

  /* exec only returns on failure. */
  execvpe(cs->argv[0], (char *const *)cs->argv, (char *const *)cs->envp);
  fatal_perror("execvpe");
}

static int
run_isolated(child_state *cs, const sigset_t *mask)
{
  pid_t child;
  int sig;
  siginfo_t si;
  struct timespec timeout, before, after;
  bool death_pending = false;

  fflush(0);
  child = fork();
  if (child == -1)
    fatal_perror("fork");
  if (child == 0)
    run_isolated_child(cs);

  /* We are the parent. */
  child_pgrp = child;
  timeout.tv_nsec = 0;
  timeout.tv_sec = ISOLATE_RLIMIT_WALL;

  for (;;) {
    if (clock_gettime(CLOCK_MONOTONIC, &before))
      fatal_perror("clock_gettime: CLOCK_MONOTONIC");

    sig = sigtimedwait(mask, &si, &timeout);

    if (clock_gettime(CLOCK_MONOTONIC, &after))
      fatal_perror("clock_gettime: CLOCK_MONOTONIC");

    /* this updates 'timeout' to the time remaining to wait, if any */
    timeout = timespec_minus(timeout, timespec_minus(after, before));
    timeout = timespec_clamp(timeout);

    switch (sig) {
    case SIGCHLD:
      /* The status is already available from the siginfo_t, but we still
         need to call waitpid to reap the zombie. */
      if (waitpid(si.si_pid, 0, WNOHANG) != si.si_pid)
        fatal_perror("waitpid");

      if (si.si_pid != child)
        /* We shouldn't have any other children, but whatever. */
        continue;

      /* The child we were waiting for has exited; it is not necessary
         to kill it on the way out. */
      child_pgrp = 0;

      if (si.si_code == CLD_EXITED)
        /* We're done. */
        return si.si_status;

      if (si.si_code == CLD_KILLED || si.si_code == CLD_DUMPED)
        fatal_printf("%s: %s%s",
                     cs->argv[0], strsignal(si.si_status),
                     si.si_code == CLD_DUMPED ? " (core dumped)" : "");

      /* Other values of si.si_code should not occur. */
      fatal_printf("Unexpected child status change: si_code=%d si_status=%d",
                   si.si_code, si.si_status);

    case SIGTSTP:
    case SIGTTIN:
    case SIGTTOU:
      killpg(child_pgrp, sig);
      raise(SIGSTOP);
      killpg(child_pgrp, SIGCONT);
      continue;

    case -1: /* timeout */
      if (errno != EAGAIN)
        fatal_perror("sigtimedwait");
      sig = death_pending ? SIGKILL : SIGALRM;
      /* fall through */

    default:
      killpg(child_pgrp, sig);
      death_pending = 1;
      timeout.tv_nsec = 0;
      timeout.tv_sec = 1;
    }
  }
}

int
main(int UNUSED(argc), char **argv, char **envp)
{
  child_state cs;
  sigset_t parent_sigmask;
  char stderr_buffer[BUFSIZ];

  progname = strrchr(argv[0], '/');
  if (progname)
    progname++;
  else
    progname = argv[0];

  /* Line-buffer stderr so that any error messages we emit are
     atomically written (all of our messages are exactly one line). */
  setvbuf(stderr, stderr_buffer, _IOLBF, BUFSIZ);

  if (atexit(cleanups))
    fatal_perror("atexit");

  memset(&cs, 0, sizeof cs);
  cs.argv = (const char **)(argv + 1);

  prepare_fds();
  prepare_signals(&cs, &parent_sigmask);
  prepare_homedir(&cs);
  prepare_environment(&cs, envp);

  return run_isolated(&cs, &parent_sigmask);
}
