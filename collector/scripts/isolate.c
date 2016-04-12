/* Wrapper for invoking programs in a weakly isolated environment.
 *
 * Copyright © 2014 Zack Weinberg
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * There is NO WARRANTY.
 *
 *    isolate [VAR=val...] program [args...]
 *
 * runs 'program' with arguments 'args' under its own user and group
 * ID, in a just-created, (almost) empty home directory, in its own
 * background process group.  stdin, stdout, and stderr are inherited
 * from the parent, and all other file descriptors are closed.
 * Resource limits are applied (see below).  When 'program' exits,
 * everything else in its process group is killed, and its home
 * directory is erased.
 *
 * HOME, USER, PWD, LOGNAME, and SHELL are set appropriately; TMPDIR
 * is set to $HOME/.tmp, which is created along with $HOME; PATH, TZ,
 * TERM, LANG, and LC_* are preserved; all other environment variables
 * are cleared.  'VAR=val' arguments to isolate, prior to 'program',
 * set additional environment variables for 'program', a la env(3).
 * The first argument that does not match /^[A-Za-z_][A-Za-z0-9_]*=/
 * is taken as 'program', and all subsequent arguments are passed to
 * 'program' verbatim.
 *
 * VARs with names starting ISOL_*, on the command line, may be used
 * to adjust the behavior of this program, and will not be passed
 * down.  These are *not* honored if set in this program's own
 * environment variable block.  Unrecognized ISOL_* variables are a
 * fatal error.
 *
 * This program is to be installed setuid root.
 *
 * The directory ISOL_HOME (default /home/isolated) must exist, be
 * owned by root, and not be used for any other purpose.
 *
 * The userid range ISOL_LOW_UID (default 2000) through ISOL_HIGH_UID
 * (default 2999), inclusive, must not conflict with any existing user
 * or group ID.  If you put this uid range in /etc/passwd and
 * /etc/group, the username, group membership and shell specified
 * there (but *not* the homedir) will be honored; otherwise, the
 * process will be given a primary GID with the same numeric value as
 * its UID, no supplementary groups, USER and LOGNAME will be set to
 * "iso-NNNN" where NNNN is the decimal UID, and SHELL will be set to
 * "/bin/sh".
 *
 * If ISOL_NETNS is set to any value, this program reexecs itself
 * under "ip netns exec $value" before doing anything else, thus
 * arranging for the subsidiary program to run in a non-default
 * network namespace (which must already have been established).
 *
 * There are twelve parameters of the form "ISOL_RL_<limit>" -- see
 * below for a list -- which can be used to set resource limits on the
 * isolated program.  Most, but not all, of the <limit>s correspond to
 * RLIM_<limit> constants from sys/resource.h and are enforced via
 * setrlimit(2).  The exceptions are ISOL_RL_WALL, which places a
 * limit on *wall-clock* execution time (enforced by watchdog timer in
 * the parent process) and ISOL_RL_MEM, which sets all three of
 * RLIMIT_AS, RLIMIT_DATA, and RLIMIT_RSS; those three cannot be set
 * individually.
 *
 * This program is not intended as a replacement for full-fledged
 * containers!  The subsidiary program can still access the entire
 * filesystem and all other shared resources.  It can spawn children
 * that remove themselves from its process group, and thus escape
 * termination when their parent exits.  There is no attempt to set
 * extended credentials of any kind, or apply PAM session settings, or
 * anything like that.  But on the up side, you don't have to
 * construct a chroot environment.
 *
 * This program has only been tested on Linux.  C99 and POSIX.1-2001
 * features are used throughout.  It also requires static_assert, from
 * C11; dirfd, lchown, and strdup, from POSIX.1-2008; and execvpe,
 * initgroups, and vasprintf, from the shared BSD/GNU extension set.
 *
 * It should not be difficult to port this program to any modern *BSD,
 * but it may well be impractical to port it to anything older.
 */

#define _GNU_SOURCE 1
#define _FILE_OFFSET_BITS 64 /* large directory readdir(), large rlimits */

#undef NDEBUG
#include <assert.h>

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
#include <regex.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
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

/* Child process defaults.  May be overridden on the command line,
   see prepare_args.  */
#define ISOL_HOME        "/home/isolated"
#define ISOL_LOW_UID     2000
#define ISOL_HIGH_UID    2999
#define ISOL_UMASK       077      /* umask for command */

/* Child process rlimits.  May also be overridden on the command line,
   see prepare_args. Note that not all of these are enforced via
   setrlimit(), and we attempt to gloss over some of the awkwardness
   in memory-limit handling.  We always set the hard and soft limits
   to whatever value is specified.  */

/* We need to stick some special-case values into the RLIMIT_* number space. */
static_assert(
              /* POSIX.1-2008 constants */
              RLIMIT_CORE       >= 0 &&
              RLIMIT_CPU        >= 0 &&
              RLIMIT_DATA       >= 0 &&
              RLIMIT_FSIZE      >= 0 &&
              RLIMIT_NOFILE     >= 0 &&
              RLIMIT_STACK      >= 0 &&
              RLIMIT_AS         >= 0 &&
              /* Extensions */
#ifdef RLIMIT_MEMLOCK
              RLIMIT_MEMLOCK    >= 0 &&
#endif
#ifdef RLIMIT_MSGQUEUE
              RLIMIT_MSGQUEUE   >= 0 &&
#endif
#ifdef RLIMIT_NICE
              RLIMIT_NICE       >= 0 &&
#endif
#ifdef RLIMIT_NPROC
              RLIMIT_NPROC      >= 0 &&
#endif
#ifdef RLIMIT_RSS
              RLIMIT_RSS        >= 0 &&
#endif
#ifdef RLIMIT_SIGPENDING
              RLIMIT_SIGPENDING >= 0 &&
#endif
              1, "negative rlimit constants break the rlimit_defaults table");

/* For caller's convenience, if the nonstandard rlimit constants are
   not defined, we still accept (but ignore) the corresponding
   ISOL_RL_* argument. */
#ifndef RLIMIT_MEMLOCK
#define RLIMIT_MEMLOCK -1
#endif
#ifndef RLIMIT_MSGQUEUE
#define RLIMIT_MSGQUEUE -1
#endif
#ifndef RLIMIT_NICE
#define RLIMIT_NICE -1
#endif
#ifndef RLIMIT_NPROC
#define RLIMIT_NPROC -1
#endif
#ifndef RLIMIT_RSS
#define RLIMIT_RSS -1
#endif
#ifndef RLIMIT_SIGPENDING
#define RLIMIT_SIGPENDING -1
#endif

/* Special case: wall clock time limit enforced via the loop in run_isolated */
#define PSEUDO_RLIMIT_WALL -2
/* Special case: sets RLIMIT_AS, _DATA, and _RSS all to the same value */
#define PSEUDO_RLIMIT_MEM -3

typedef struct rlimit_def_entry
{
  /* Making these embedded arrays wastes a small amount of space but
     eliminates all data-segment relocations. */
  const char var[sizeof "ISOL_RL_SIGPENDING="];
  const char vareq[sizeof "ISOL_RL_SIGPENDING="];
  size_t vareqlen;
  int resource;
  rlim_t value;
}
rlimit_def_entry;

#define N_RLIMITS 12
#define N_RLIM_WALL 0

static const struct rlimit_def_entry rlimit_defaults[N_RLIMITS] = {
#define RLD(name, def) \
  { "ISOL_RL_" #name, "ISOL_RL_" #name "=", sizeof "ISOL_RL_" #name, \
    RLIMIT_##name, def }
#define PRLD(name, def) \
  { "ISOL_RL_" #name, "ISOL_RL_" #name "=", sizeof "ISOL_RL_" #name, \
    PSEUDO_RLIMIT_##name, def }

  /* time */
  PRLD(WALL,       600),             /* ten minutes */
  RLD (CPU,         60),             /* one minute */

  /* storage */
  RLD (CORE,       0),               /* no core dumps */
  RLD (MEMLOCK,    ((rlim_t)1)<<16), /* 64 kilobytes */
  RLD (MSGQUEUE,   ((rlim_t)1)<<20), /*  1 megabyte */
  RLD (STACK,      ((rlim_t)1)<<25), /* 32 megabytes */
  PRLD(MEM,        ((rlim_t)1)<<31), /*  2 gigabytes */
  RLD (FSIZE,      ((rlim_t)1)<<33), /*  8 gigabytes */

  /* misc */
  RLD (NICE,       0),               /* no elevated priorities */
  RLD (SIGPENDING, 64111),           /* kernel 3.14.5 default */
  RLD (NPROC,      1024),            /* 1 kilo - smaller than default */
  RLD (NOFILE,     ((rlim_t)1)<<20), /* 1 mega - bigger than default */

#undef RLD
#undef PRLD
};

/* Child process state. */
typedef struct child_state
{
  const char *homedir;
  const char *logname;
  const char *shell;
  const char *const *argv;
  const char **envp;

  sigset_t sigmask;
  uid_t uid;
  gid_t gid;
  mode_t umask;
  rlim_t rlimits[N_RLIMITS];

  uid_t low_uid_range;
  uid_t high_uid_range;
  const char *homedir_base;
}
child_state;

static_assert(N_RLIMITS == sizeof(rlimit_defaults)/sizeof(rlimit_def_entry),
              "N_RLIMITS is wrong");

static void
init_child_state(child_state *cs)
{
  memset(cs, 0, sizeof(child_state));

  cs->uid = (uid_t)-1;
  cs->gid = (gid_t)-1;
  cs->umask = ISOL_UMASK;
  for (int i = 0; i < N_RLIMITS; i++)
    cs->rlimits[i] = rlimit_defaults[i].value;

  cs->low_uid_range  = ISOL_LOW_UID;
  cs->high_uid_range = ISOL_HIGH_UID;
  cs->homedir_base   = ISOL_HOME;
}

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

static void
x_append_strvec(const char ***vecp,
                size_t *allocp,
                size_t *usedp,
                const char *val)
{
  size_t used = *usedp;
  size_t alloc = *allocp;
  const char **vec = *vecp;

  if (used >= alloc) {
    if (alloc == 0)
      alloc = 8;
    else
      alloc *= 2;

    vec = xreallocarray(vec, alloc, sizeof(char *));
    *vecp = vec;
    *allocp = alloc;
  }

  vec[used++] = val;
  *usedp = used;
}

static long long
xstrtonum_base(const char *str, long long minval, long long maxval,
               const char *msgprefix, int base)
{
  if (minval > maxval)
    fatal_printf("xstrtonum: misuse: minval(%lld) > maxval(%lld)",
                 minval, maxval);

  long long rv;
  char *endp;
  errno = 0;
  rv = strtoll(str, &endp, base);
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
#define xstrtonum(s, mi, mx, ms) xstrtonum_base(s, mi, mx, ms, 10)
#define xstrtonum_oct(s, mi, mx, ms) xstrtonum_base(s, mi, mx, ms, 8)

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
    const char *const rm_args[] = {
      "rm", "-rf", homedir, 0
    };
    const char *const noenv[] = { 0 };
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
    closedir(fdir);

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

  for (u = cs->low_uid_range; u <= cs->high_uid_range; u++) {
    h = xasprintf("%s/%u", cs->homedir_base, u);
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
  if (cs->gid == (gid_t)-1)
    cs->gid = cs->uid;

  if (lchown(h, cs->uid, cs->gid))
    fatal_eprintf("lchown: %s", h);
}

#define startswith(x, y) (!strncmp((x), (y), sizeof(y) - 1))

typedef enum { NOT_VARVAL = 0, ENV_VARVAL = 1, ISOL_VARVAL = 2 } varval_kind;

static varval_kind
classify_as_varval(const char *arg)
{
  static regex_t is_varval;
  static bool is_varval_ready = false;
  if (!is_varval_ready) {
    int err = regcomp(&is_varval, "^[A-Za-z_][A-Za-z0-9_]*=", REG_NOSUB);
    if (err)
      fatal_regerror("regcomp", err, &is_varval);
    is_varval_ready = true;
  }

  int match = regexec(&is_varval, arg, 0, 0, 0);
  if (match == REG_NOMATCH)
    return NOT_VARVAL;
  else if (match == 0)
    return startswith(arg, "ISOL_") ? ISOL_VARVAL : ENV_VARVAL;
  else
    fatal_regerror("regexec", match, &is_varval);
}

static inline const char *
extract_isol_val(const char *var,
                 const char *vareq,
                 size_t vareqlen,
                 const char *arg)
{
  if (!strncmp(vareq, arg, vareqlen)) {
    if (!strcmp(vareq, arg))
      fatal_printf("%s may not be set to the empty string", var);
    return arg + vareqlen;
  }
  return 0;
}
#define if_isvar(var, arg, val)                                 \
  if ((val = extract_isol_val(var, var"=", sizeof var, arg)))

static void
process_isol_varval(child_state *cs, const char *arg)
{
  const char *val;
  if_isvar("ISOL_HOME", arg, val) {
    cs->homedir_base = val;
    return;
  }

  if_isvar("ISOL_UMASK", arg, val) {
    cs->umask = (mode_t)xstrtonum_oct(val, 0, 0777, "invalid umask value");
    return;
  }

  if_isvar("ISOL_LOW_UID", arg, val) {
    cs->low_uid_range = (uid_t)xstrtonum(val, 0, INT_MAX, "invalid user ID");
    return;
  }

  if_isvar("ISOL_HIGH_UID", arg, val) {
    cs->high_uid_range = (uid_t)xstrtonum(val, 0, INT_MAX, "invalid user ID");
    return;
  }

  for (int i = 0; i < N_RLIMITS; i++) {
    const struct rlimit_def_entry *def = &rlimit_defaults[i];
    if ((val = extract_isol_val(def->var, def->vareq, def->vareqlen, arg))) {
      if (!strcmp(val, "unlimited"))
        cs->rlimits[i] = RLIM_INFINITY;
      else {
        /* The value of RLIM_INFINITY is unspecified, as is the signedness
           of rlim_t, and there are no RLIM_MIN/RLIM_MAX constants.  However,
           we do know a priori that rlimits less than zero don't make sense. */
        long long rv = xstrtonum(val, 0, LLONG_MAX, "invalid rlimit value");
        if ((unsigned long long)(rlim_t)rv != (unsigned long long)rv ||
            (rlim_t)rv == RLIM_INFINITY)
          fatal_printf("%s: rlimit value out of range", arg);
        cs->rlimits[i] = (rlim_t)rv;
      }
      return;
    }
  }

  fatal_printf("unrecognized command line argument: %s", arg);
}

/* These are the only environment variables that we accept as safe to
   receive from our parent. */
static inline bool
preserve_envvar(const char *envvar)
{
  return (startswith(envvar, "PATH=") ||
          startswith(envvar, "TZ=") ||
          startswith(envvar, "TERM=") ||
          startswith(envvar, "LANG=") ||
          startswith(envvar, "LC_"));
}

/* These environment variables may not be set for children on the
   command line, either because we're going to set them ourselves, or
   because they should've been set in the parent environment instead. */
static inline bool
not_allowed_on_cmdline_envvar(const char *envvar)
{
  return (preserve_envvar(envvar) ||
          startswith(envvar, "HOME=") ||
          startswith(envvar, "PWD=") ||
          startswith(envvar, "TMPDIR=") ||
          startswith(envvar, "USER=") ||
          startswith(envvar, "LOGNAME=") ||
          startswith(envvar, "SHELL="));
}

static void
prune_environment(char **envp)
{
    size_t i, j;

    for (i = 0, j = 0; envp[i]; i++)
        if (preserve_envvar(envp[i]))
            envp[j++] = envp[i];

    envp[j] = 0;
}

static void
prepare_child_argv_envp(child_state *cs, const char *const *argv)
{
  const char **envp = 0;
  size_t alloc = 0;
  size_t used = 0;

  for (size_t i = 1; argv[i]; i++) {
    varval_kind vk = classify_as_varval(argv[i]);
    if (vk == NOT_VARVAL) {
      cs->argv = argv+i;
      x_append_strvec(&envp, &alloc, &used, 0);
      cs->envp = envp;
      break;
    }
    else if (vk == ENV_VARVAL) {
      if (not_allowed_on_cmdline_envvar(argv[i]))
        fatal_printf("may not be set on command line: %s", argv[i]);
      x_append_strvec(&envp, &alloc, &used, argv[i]);
    }
    else
      process_isol_varval(cs, argv[i]);
  }

  if (cs->low_uid_range > cs->high_uid_range)
    fatal("ISOL_LOW_UID may not be set greater than ISOL_HIGH_UID");
}


static int
compar_str(const void *a, const void *b)
{
  return strcmp(*(const char *const *)a, *(const char *const *)b);
}

static void
finish_child_argv_envp(child_state *cs, const char *const *envp)
{
  size_t envc;
  size_t aenvc;
  size_t i;
  char *tmpdir;
  const char **nenvp = cs->envp;

  tmpdir = xasprintf("%s/.tmp", cs->homedir);
  if (mkdir(tmpdir, 0700))
    fatal_eprintf("mkdir: %s", tmpdir);
  if (lchown(tmpdir, cs->uid, cs->gid))
    fatal_eprintf("lchown: %s", tmpdir);

  envc = 0;
  if (nenvp) {
    do {} while (nenvp[envc++]);
    aenvc = envc--; /* close enough */
  }

  for (i = 0; envp[i]; i++)
    if (preserve_envvar(envp[i]))
      x_append_strvec(&nenvp, &aenvc, &envc, envp[i]);


  /* Six environment variables are force-set:
     HOME PWD TMPDIR USER LOGNAME SHELL.
     The terminator is already included in aenvc. */

  x_append_strvec(&nenvp, &aenvc, &envc, xasprintf("HOME=%s", cs->homedir));
  x_append_strvec(&nenvp, &aenvc, &envc, xasprintf("PWD=%s", cs->homedir));
  x_append_strvec(&nenvp, &aenvc, &envc, xasprintf("TMPDIR=%s", tmpdir));
  x_append_strvec(&nenvp, &aenvc, &envc, xasprintf("USER=%s", cs->logname));
  x_append_strvec(&nenvp, &aenvc, &envc, xasprintf("LOGNAME=%s", cs->logname));
  x_append_strvec(&nenvp, &aenvc, &envc, xasprintf("SHELL=%s", cs->shell));
  x_append_strvec(&nenvp, &aenvc, &envc, 0);

  free(tmpdir);
  qsort(nenvp, envc-1, sizeof(char *), compar_str);
  cs->envp = nenvp;
}

/* "ip netns exec" does a fair amount of black magic, so we delegate
   to it for handling of ISOL_NETNS=.  */
static void
maybe_switch_network_namespace(int argc, char **argv, char **envp)
{
  const char *val;
  int i, j;
  for (i = 1; i < argc; i++)
    if_isvar("ISOL_NETNS", argv[i], val)
      goto found;
  return;

 found:
  for (i++; argv[i]; i++)
    if (startswith(argv[i], "ISOL_NETNS="))
      fatal("ISOL_NETNS may not be used twice");

  /* { "ip", "netns", "exec", val, argv[0], ... argv[argc] }
     but with the ISOL_NETNS= argument removed. */
  const char **nargv = xreallocarray(0, ((size_t)argc) + 1 - 1 + 4,
                                     sizeof(char *));
  nargv[0] = "ip";
  nargv[1] = "netns";
  nargv[2] = "exec";
  nargv[3] = val;
  for (i = 0, j = 4; i <= argc; i++)
    if (!argv[i] || !startswith(argv[i], "ISOL_NETNS="))
      nargv[j++] = argv[i];

  execvpe(nargv[0], (char *const *)nargv, (char *const *)envp);
  fatal_perror("execvpe");
}

static void
set_one_rlimit(int resource, rlim_t value)
{
  if (resource == PSEUDO_RLIMIT_MEM) {
    set_one_rlimit(RLIMIT_AS, value);
    set_one_rlimit(RLIMIT_DATA, value);
    set_one_rlimit(RLIMIT_RSS, value);
  } else if (resource < 0) {
    /* either not defined by this OS,
       or a pseudo-resource we don't handle here */
    return;
  } else {
    struct rlimit rl;

    rl.rlim_cur = value;
    rl.rlim_max = value;
    if (setrlimit(resource, &rl))
      fatal_eprintf("setrlimit(%d)", resource);
  }
}

static NORETURN
run_isolated_child(child_state *cs)
{
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
  for (int i = 0; i < N_RLIMITS; i++)
    set_one_rlimit(rlimit_defaults[i].resource, cs->rlimits[i]);

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
  umask(cs->umask);

  /* exec only returns on failure. */
  execvpe(cs->argv[0], (char *const *)cs->argv, (char *const *)cs->envp);
  fatal_perror("execvpe");
}

static NORETURN
run_isolated(child_state *cs, const sigset_t *mask)
{
  pid_t child;
  int sig;
  siginfo_t si;
  struct timespec timeout, before, after;
  bool death_pending = false;

  /* A few sanity checks can only be carried out at this point. */
  if (cs->uid == (uid_t)-1 || cs->gid == (gid_t)-1)
    fatal("user and group ID were not assigned");
  assert(rlimit_defaults[N_RLIM_WALL].resource == PSEUDO_RLIMIT_WALL);

  fflush(0);
  child = fork();
  if (child == -1)
    fatal_perror("fork");
  if (child == 0)
    run_isolated_child(cs);

  /* We are the parent. */
  child_pgrp = child;
  timeout.tv_nsec = 0;
  timeout.tv_sec = (time_t)cs->rlimits[N_RLIM_WALL];

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

      if (si.si_code == CLD_EXITED) {
        if (si.si_status == 0)
          exit(0);
        else
          fatal_printf("%s: unsuccessful exit code %d",
                       cs->argv[0], si.si_status);
      }

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
main(int argc, char **argv, char **envp)
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
     atomically written (all of our messages are exactly one line).
     Then close all unnecessary file descriptors and prune the
     environment variables; both of these sanitization tasks must
     happen before any I/O or subprocess invocation can occur.  */
  setvbuf(stderr, stderr_buffer, _IOLBF, BUFSIZ);
  prepare_fds();
  prune_environment(envp);

  if (argc < 2) {
    fprintf(stderr, "usage: %s [VAR=val...] program [args...]\n", progname);
    return 2;
  }

  if (geteuid())
    fatal("must be run as root");

  /* Network namespace switching reexecutes this program under
     "ip netns exec" with a modified command line, so we do it
     before processing the command line for anything else. */
  maybe_switch_network_namespace(argc, argv, envp);

  init_child_state(&cs);
  prepare_signals(&cs, &parent_sigmask);
  prepare_child_argv_envp(&cs, (const char *const *)argv);

  /* Prepare the isolation environment and launch the child. */
  if (atexit(cleanups))
    fatal_perror("atexit");
  prepare_homedir(&cs);
  finish_child_argv_envp(&cs, (const char *const *)envp);
  run_isolated(&cs, &parent_sigmask);
}
