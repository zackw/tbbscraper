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
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
} child_state;

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

static void
run_phantomjs(child_state *cs)
{
  struct rlimit rl;

  /* This code executes on the child side of a fork(), but the
     parent has arranged for it to be safe for us to write to
     stderr under error conditions. */
  if (chdir(cs->homedir))
    fatal_eprintf("chdir: %s", cs->homedir);

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

  /* Wall-clock timeout. alarm() cannot fail. */
  alarm(PHANTOMJS_RLIMIT_WALL);

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
  pid_t child;
  int status;
  child_state cs;
  memset(&cs, 0, sizeof cs);

  select_homedir(&cs);
  prepare_environment(&cs, argv, envp);

  fflush(0);
  child = fork();
  if (child == -1)
    fatal_perror("fork");

  if (child == 0)
    run_phantomjs(&cs);

  if (waitpid(child, &status, 0) != child)
    fatal_perror("waitpid");

  cleanup_homedir(&cs);

  if (!WIFEXITED(status))
    fatal_printf("child process killed by signal %d\n", WTERMSIG(status));

  return WEXITSTATUS(status);
}
