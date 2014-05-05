/* Wrapper for invoking phantomjs in a chroot.

   This process runs as root inside the chroot, which has been set up
   to have N home directories.  It scans for the first one that does
   not currently have a .qws directory (this directory is used for
   scratch storage by phantomjs).  When it finds a homedir in which it
   can create a .qws directory, it creates that directory and then
   invokes phantomjs in that homedir under that uid:gid, with $HOME
   set appropriately, passing down argv[], stdin, stdout, and stderr.
   When the phantomjs process terminates, the contents of the homedir
   are erased (shelling out to find(1) to do so) the .qws directory is
   also deleted, and then this process terminates, passing up the
   child's exit code.  */

#define _GNU_SOURCE
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

/* Compile-time configuration knobs.  This program is to be invoked as
   root, so runtime configuration has been minimized. */
#ifndef PHANTOMJS_PATH
#define PHANTOMJS_PATH "/bin"
#endif
#ifndef PHANTOMJS_BINARY
#define PHANTOMJS_BINARY "/bin/phantomjs"
#endif
#ifndef PHANTOMJS_FIND_BINARY
#define PHANTOMJS_FIND_BINARY "/bin/find"
#endif
#ifndef PHANTOMJS_RLIMIT_CPU
#define PHANTOMJS_RLIMIT_CPU 60 /* one minute */
#endif
#ifndef PHANTOMJS_RLIMIT_WALL
#define PHANTOMJS_RLIMIT_WALL 600 /* ten minutes */
#endif
#ifndef PHANTOMJS_RLIMIT_MEM
#define PHANTOMJS_RLIMIT_MEM (1L<<30) /* one gigabyte */
#endif
#ifndef PHANTOMJS_RLIMIT_CORE
#define PHANTOMJS_RLIMIT_CORE 0 /* no core dumps */
#endif

#if defined __GNUC__ && __GNUC__ >= 4
#define NORETURN void __attribute__((noreturn))
#define PRINTFLIKE __attribute__((format(printf,1,2)))
#define UNUSED(arg) arg __attribute__((unused))
#else
#define NORETURN void
#define PRINTFLIKE /*nothing*/
#define UNUSED(arg) arg
#endif

typedef struct child_state
{
  const char *homedir;
  const char **argv;
  const char **envp;
  uid_t uid;
  gid_t gid;
  sigset_t sigmask;
} child_state;

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

/* Error reporting. */

static NORETURN
fatal(const char *msg)
{
  fprintf(stderr, "phantomjs-wrapper: %s\n", msg);
  exit(1);
}

static NORETURN
fatal_perror(const char *msg)
{
  fprintf(stderr, "phantomjs-wrapper: %s: %s\n", msg, strerror(errno));
  exit(1);
}

static PRINTFLIKE NORETURN
fatal_printf(const char *msg, ...)
{
  va_list ap;
  fputs("phantomjs-wrapper: ", stderr);
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
  fputs("phantomjs-wrapper: ", stderr);
  va_list ap;
  va_start(ap, msg);
  vfprintf(stderr, msg, ap);
  va_end(ap);
  fprintf(stderr, ": %s\n", strerror(err));
  exit(1);
}

static void
prepare_signals(child_state *cs, sigset_t *parent_sigmask)
{
  /* save current signal mask for child */
  if (sigprocmask(0, 0, &cs->sigmask))
    fatal_perror("sigprocmask");

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

  if (sigprocmask(SIG_SETMASK, &parent_sigmask, 0))
    fatal_perror("sigprocmask");
}

static void
select_homedir(child_state *cs)
{
  DIR *dirp;
  char *homedir;
  char *qws;
  struct dirent dent, *dent_out;
  struct stat st;

  if (!(dirp = opendir("/home")))
    fatal_perror("opendir: /home");
  for (;;) {
    if ((errno = readdir_r(dirp, &dent, &dent_out)) != 0)
      fatal_perror("readdir: /home");

    if (!dent_out)
      fatal("no usable home directory found");

    if (dent.d_name[0] == '\0' || dent.d_name[0] == '.'
#ifdef _DIRENT_HAVE_D_TYPE
        || (dent.d_type != DT_UNKNOWN && dent.d_type != DT_DIR)
#endif
        )
      continue;

    if (asprintf(&qws, "/home/%s/.qws", dent.d_name) == -1)
      fatal_perror("asprintf");

    if (!mkdir(qws, 0700))
      break; /* success! */

    if (errno != EEXIST)
      fatal_eprintf("mkdir: %s", qws);

    free(qws);
  }
  closedir(dirp);

  if (asprintf(&homedir, "/home/%s", dent.d_name) == -1) {
    int err = errno;
    rmdir(qws);
    errno = err;
    fatal_perror("asprintf");
  }

  if (stat(homedir, &st)) {
    int err = errno;
    rmdir(qws);
    errno = err;
    fatal_eprintf("stat: %s", homedir);
  }

  /* phantomjs will run under the user and group ID that owns the
     selected home directory.  This had better not be root. */
  if (!st.st_uid || !st.st_gid) {
    rmdir(qws);
    fatal_printf("%s: owned by uid %d group %d - configuration botch",
                 homedir, st.st_uid, st.st_gid);
  }

  /* The .qws directory we just created is owned by root: change it to belong
     to the user that owns the selected home directory. */
  if (lchown(qws, cs->uid, cs->gid)) {
    int err = errno;
    rmdir(qws);
    errno = err;
    fatal_eprintf("lchown: %s", qws);
  }

  cs->uid = st.st_uid;
  cs->gid = st.st_gid;
  cs->homedir = homedir;
  free(qws);
}

static inline bool
should_copy_envvar(char *envvar)
{
#define startswith(x, y) (!strncmp((x), (y), sizeof(y) - 1))
  return !(startswith(envvar, "HOME=") ||
           startswith(envvar, "PWD=") ||
           startswith(envvar, "USER=") ||
           startswith(envvar, "LOGNAME=") ||
           startswith(envvar, "PATH=") ||
           startswith(envvar, "SHELL=") ||
           startswith(envvar, "SCHROOT_"));
#undef startswith
}

static char *
make_envvar(const char *name, const char *value)
{
  char *rv;
  if (asprintf(&rv, "%s=%s", name, value) == -1)
    fatal_perror("asprintf");
  return rv;
}

static int
compar_str(const void *a, const void *b)
{
  return strcmp(*(char *const *)a, *(char *const *)b);
}

static void
prepare_environment(child_state *cs, char **argv, char **envp)
{
  struct passwd *pw;
  size_t argc;
  size_t envc;
  size_t i, j;

  /* We copy argv so we can replace argv[0] and inject some options.  */
  for (argc = 0; argv[argc]; argc++);
  cs->argv = malloc((argc + 2) * sizeof(char *));
  if (!cs->argv)
    fatal_perror("malloc");

  i = 0;
  cs->argv[i++] = "phantomjs";
  cs->argv[i++] = "--ssl-protocol=any";
  for (j = 1; j <= argc; i++, j++)
    cs->argv[i] = argv[j];

  /* The environment is a little more work. We want to reset HOME,
     PWD, USER, LOGNAME, PATH, and SHELL to the correct values for the
     user 'phantomjs' will run as, discard all environment variables
     starting with SCHROOT_, and leave everything else alone. */
  pw = getpwuid(cs->uid);
  if (!pw)
    fatal_perror("getpwuid");

  /* sanity check */
  if (pw->pw_uid != cs->uid || pw->pw_gid != cs->gid)
    fatal_printf("wrong user/group for %s: homedir %d:%d, passwd %d:%d",
                 pw->pw_name, cs->uid, cs->gid, pw->pw_uid, pw->pw_gid);

  if (strcmp(pw->pw_shell, "/bin/sh"))
    fatal_printf("wrong shell for user %s (uid %d): %s",
                 pw->pw_name, cs->uid, pw->pw_shell);

  if (strcmp(pw->pw_dir, cs->homedir))
    fatal_printf("wrong homedir for user %s: found %s, passwd %s",
                 pw->pw_name, cs->homedir, pw->pw_dir);

  envc = 0;
  for (i = 0; envp[i]; i++)
    if (should_copy_envvar(envp[i]))
      envc++;

  /* Six environment variables are force-set:
     HOME PWD USER LOGNAME PATH SHELL.
     One more for the terminator. */
  cs->envp = malloc((envc + 7) * sizeof(char *));
  if (!cs->envp)
    fatal_perror("malloc");

  envc = 0;
  for (i = 0; envp[i]; i++)
    if (should_copy_envvar(envp[i]))
      cs->envp[envc++] = envp[i];

  cs->envp[envc++] = "SHELL=/bin/sh";
  cs->envp[envc++] = "PATH=" PHANTOMJS_PATH;
  cs->envp[envc++] = make_envvar("HOME", cs->homedir);
  cs->envp[envc++] = make_envvar("PWD", cs->homedir);
  cs->envp[envc++] = make_envvar("USER", pw->pw_name);
  cs->envp[envc++] = make_envvar("LOGNAME", pw->pw_name);
  cs->envp[envc] = 0;

  qsort(cs->envp, envc, sizeof(char *), compar_str);
}

static NORETURN
run_phantomjs_child(child_state *cs)
{
  struct rlimit rl;

  /* This code executes on the child side of a fork(), but the
     parent has arranged for it to be safe for us to write to
     stderr under error conditions. */
  if (chdir(cs->homedir))
    fatal_eprintf("chdir: %s", cs->homedir);

  /* Reset signal handling. */
  if (sigprocmask(SIG_SETMASK, &cs->sigmask, 0))
    fatal_perror("sigprocmask");

  /* Apply resource limits. */
  rl.rlim_cur = PHANTOMJS_RLIMIT_CPU;
  rl.rlim_max = PHANTOMJS_RLIMIT_CPU;
  if (setrlimit(RLIMIT_CPU, &rl))
    fatal_perror("setrlimit(RLIMIT_CPU)");

  rl.rlim_cur = PHANTOMJS_RLIMIT_MEM;
  rl.rlim_max = PHANTOMJS_RLIMIT_MEM;
  if (setrlimit(RLIMIT_AS, &rl))
    fatal_perror("setrlimit(RLIMIT_AS)");
  if (setrlimit(RLIMIT_DATA, &rl))
    fatal_perror("setrlimit(RLIMIT_DATA)");

  rl.rlim_cur = PHANTOMJS_RLIMIT_CORE;
  rl.rlim_max = PHANTOMJS_RLIMIT_CORE;
  if (setrlimit(RLIMIT_CORE, &rl))
    fatal_perror("setrlimit(RLIMIT_CORE)");

  /* Wall-clock timeout is applied by the parent. */

  /* Drop privileges. */
  if (setgroups(0, 0))
    fatal_perror("setgroups");
  if (setgid(cs->gid))
    fatal_perror("setgid");
  if (setuid(cs->uid))
    fatal_perror("setuid");

  /* execve() only returns on failure. */
  execve(PHANTOMJS_BINARY, (char *const *)cs->argv, (char *const *)cs->envp);
  fatal_perror("execve");
}

/* Reap all exited children.  If one of them was EXPECTED_CHILD,
   then set *STATUSP to its exit status and return true; else
   return false.  We shouldn't ever have any children other than
   the expected one; this is extra defensiveness. */
static bool
reap_all(pid_t expected_child, int *statusp)
{
  pid_t child;
  int status;
  bool found = false;
  while ((child = waitpid(0, &status, WNOHANG)) != 0) {
    /* N.B. ECHILD here means that the process we care about somehow
       escaped monitoring. */
    if (child == -1)
      fatal_perror("waitpid");

    if (child == expected_child) {
      *pstatus = status;
      found = true;
    }
  }
  return found;
}

static int
run_phantomjs(child_state *cs, const sigset_t *mask)
{
  pid_t child;
  int status;
  int sig;
  struct timespec timeout, before, after;
  bool death_pending = false;

  fflush(0);
  child = fork();
  if (child == -1)
    fatal_perror("fork");
  if (child == 0)
    run_phantomjs_child(&cs);

  /* We are the parent. */
  timeout.tv_nsec = 0;
  timeout.tv_sec = PHANTOMJS_RLIMIT_WALL;

  for (;;) {
    if (clock_gettime(CLOCK_MONOTONIC, &before))
      fatal_perror("clock_gettime: CLOCK_MONOTONIC");

    sig = sigtimedwait(mask, 0, &timeout);

    if (clock_gettime(CLOCK_MONOTONIC, &after))
      fatal_perror("clock_gettime: CLOCK_MONOTONIC");

    /* this updates 'timeout' to the time remaining to wait */
    timeout = timespec_minus(timeout, timespec_minus(after, before));

    switch (sig) {
    case SIGCHLD:
      if (reap_all(child, &status))
        return status;
      break;

    case SIGTSTP:
    case SIGTTIN:
    case SIGTTOU:
      kill(child, sig);
      raise(SIGSTOP);
      kill(child, SIGCONT);
      continue;

    case -1: /* timeout */
      sig = death_pending ? SIGKILL : SIGALRM;
      /* fall through */

    default:
      kill(child, sig);
      death_pending = 1;
      timeout.tv_nsec = 0;
      timeout.tv_sec = 1;
    }
  }

  return status;
}

static void
cleanup_homedir(child_state *cs)
{
  /* Delete everything in the homedir that isn't the .qws directory
     (which is being used as a lockfile); only after that succeeds
     delete .qws itself.  Most of the work is outsourced to find(1).  */

  const char *find_args[] = {
    "find", ".", "-depth", "!", "-path", "./.qws", "-delete", 0
  };
  const char *noenv[] = { 0 };
  pid_t child;
  int status;

  if (chdir(cs->homedir))
    fatal_eprintf("chdir: %s", cs->homedir);

  fflush(0);
  child = fork();
  if (child == -1)
    return;
  if (child == 0) {
    if (close(0) ||
        open("/dev/null", O_RDONLY) != 0 ||
        close(1) ||
        open("/dev/null", O_WRONLY) != 1 ||
        close(2) ||
        open("/dev/null", O_WRONLY) != 2)
      _exit(126);
    execve(PHANTOMJS_FIND_BINARY,
           (char * const *)find_args, (char * const *)noenv);
    _exit(127);
  }

  if (waitpid(child, &status, 0) != child)
    fatal_perror("waitpid");

  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
    fatal_printf("find exited unsuccessfully - status %04x", status);

  if (rmdir(".qws"))
    fatal_perror("rmdir: .qws");
}

int main(int UNUSED(argc), char **argv, char **envp)
{
  int status;
  child_state cs;
  sigset_t parent_sigmask;

  memset(&cs, 0, sizeof cs);
  prepare_signals(&cs, &parent_sigmask);

  select_homedir(&cs);
  prepare_environment(&cs, argv, envp);

  status = run_phantomjs(&cs, &parent_sigmask);

  cleanup_homedir(&cs);

  if (!WIFEXITED(status))
    fatal_printf("child process killed by signal %d\n", WTERMSIG(status));

  return WEXITSTATUS(status);
}
