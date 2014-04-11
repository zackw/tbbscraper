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
#include <sys/stat.h>
#include <sys/wait.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <limits.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct child_state
{
  char *homedir;
  char **argv;
  char **envp;
  uid_t uid;
  gid_t gid;
} child_state;

static bool
select_homedir(child_state *cs)
{
  DIR *dirp;
  char pathbuf[PATH_MAX];
  struct dirent dent, *dent_out;
  struct stat st;

  if (chdir("/home")) {
    perror("chdir: /home");
    return false;
  }
  if (!(dirp = opendir("."))) {
    perror("opendir: /home");
    return false;
  }
  for (;;) {
    if ((errno = readdir_r(dirp, &dent, &dent_out))) {
      perror("readdir: /home");
      closedir(dirp);
      return false;
    }
    if (!dent_out) {
      closedir(dirp);
      return false;
    }
    if (dent.d_name[0] == '\0' || dent.d_name[0] == '.'
#ifdef _DIRENT_HAVE_D_TYPE
        || (dent.d_type != DT_UNKNOWN && dent.d_type != DT_DIR)
#endif
        )
      continue;

    if (snprintf(pathbuf, sizeof pathbuf,
                 "/home/%s/.qws", dent.d_name) >= PATH_MAX)
      continue; /* name too long, ignore it */

    if (!mkdir(pathbuf, 0777))
      break; /* success! */

    if (errno != EEXIST) {
      fprintf(stderr, "mkdir: %s: %s\n", pathbuf, strerror(errno));
      closedir(dirp);
      return false;
    }
  }
  closedir(dirp);

  cs->homedir = strdup(pathbuf);
  if (!cs->homedir) {
    perror("malloc");
    return false;
  }
  *strrchr(cs->homedir, '/') = '\0';

  if (stat(cs->homedir, &st)) {
    fprintf(stderr, "stat: %s: %s\n", cs->homedir, strerror(errno));
    return false;
  }
  /* phantomjs will run under the user and group ID that owns the
     selected home directory.  This had better not be root. */
  if (!st.st_uid || !st.st_gid) {
    fprintf(stderr, "%s owned by uid %d group %d - configuration botch\n",
            cs->homedir, st.st_uid, st.st_gid);
    return false;
  }
  cs->uid = st.st_uid;
  cs->gid = st.st_gid;
  /* The .qws directory we just created is owned by root: change it to belong
     to the user that owns the selected home directory. */
  if (lchown(pathbuf, cs->uid, cs->gid)) {
    fprintf(stderr, "lchown: %s: %s\n", pathbuf, strerror(errno));
    return false;
  }

  return true;
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
  size_t ln = strlen(name);
  size_t lv = strlen(value);
  size_t all = ln + lv + 2;
  char *rv = malloc(all);
  if (!rv) {
    perror("malloc");
    exit(1);
  }
  size_t x = snprintf(rv, all, "%s=%s", name, value);
  if (x != all-1) {
    fprintf(stderr, "expected %zu got %zu - %s\n", all-1, x, rv);
    exit(1);
  }
  return rv;
}

static int
compar_str(const void *a, const void *b)
{
  return strcmp(*(char *const *)a, *(char *const *)b);
}

static bool
prepare_environment(child_state *cs, char **argv, char **envp)
{
  struct passwd *pw;
  size_t argc;
  size_t envc;
  size_t i, j;

  /* We copy argv so we can replace argv[0] and inject some
     configuration options.  */
  for (argc = 0; argv[argc]; argc++);
  cs->argv = malloc((argc + 4) * sizeof(char *));
  if (!cs->argv) {
    perror("malloc");
    return false;
  }
  i = 0;
  cs->argv[i++] = "phantomjs";
  cs->argv[i++] = "--ignore-ssl-errors=true";
  cs->argv[i++] = "--ssl-protocol=any";
  cs->argv[i++] = "--load-images=false";
  for (j = 1; j <= argc; i++, j++)
    cs->argv[i] = argv[j];

  /* The environment is a little more work. We want to reset HOME,
     PWD, USER, LOGNAME, PATH, and SHELL to the correct values for the
     user 'phantomjs' will run as, discard all environment variables
     starting with SCHROOT_, and leave everything else alone. */
  pw = getpwuid(cs->uid);
  if (!pw) {
    perror("getpwuid");
    return false;
  }

  /* sanity check */
  if (pw->pw_uid != cs->uid || pw->pw_gid != cs->gid) {
    fprintf(stderr, "wrong user/group for %s: homedir %d:%d, passwd %d:%d\n",
            pw->pw_name, cs->uid, cs->gid, pw->pw_uid, pw->pw_gid);
  }
  if (strcmp(pw->pw_shell, "/bin/sh")) {
    fprintf(stderr, "incorrect shell for user %s (uid %d): %s\n",
            pw->pw_name, cs->uid, pw->pw_shell);
    return false;
  }
  if (strcmp(pw->pw_dir, cs->homedir)) {
    fprintf(stderr, "incorrect homedir for user %s: found %s, passwd %s\n",
            pw->pw_name, cs->homedir, pw->pw_dir);
  }

  envc = 0;
  for (i = 0; envp[i]; i++)
    if (should_copy_envvar(envp[i]))
      envc++;

  /* Six environment variables are force-set:
     HOME PWD USER LOGNAME PATH SHELL.
     One more for the terminator. */
  cs->envp = malloc((envc + 7) * sizeof(char *));
  envc = 0;
  for (i = 0; envp[i]; i++)
    if (should_copy_envvar(envp[i]))
      cs->envp[envc++] = envp[i];

  cs->envp[envc++] = "SHELL=/bin/sh";
  cs->envp[envc++] = "PATH=/bin";
  cs->envp[envc++] = make_envvar("HOME", cs->homedir);
  cs->envp[envc++] = make_envvar("PWD", cs->homedir);
  cs->envp[envc++] = make_envvar("USER", pw->pw_name);
  cs->envp[envc++] = make_envvar("LOGNAME", pw->pw_name);
  cs->envp[envc] = 0;

  qsort(cs->envp, envc, sizeof(char *), compar_str);
  return true;
}

static void
run_phantomjs(child_state *cs)
{
  /* This code executes on the child side of a fork(), but the
     parent has arranged for it to be safe for us to write to
     stderr under error conditions. */
  if (chdir(cs->homedir)) {
    fprintf(stderr, "chdir: %s: %s\n", cs->homedir, strerror(errno));
    exit(1);
  }

  /* Drop privileges. */
  if (setgroups(0, 0)) {
    perror("setgroups");
    exit(1);
  }
  if (setgid(cs->gid)) {
    perror("setgid");
    exit(1);
  }
  if (setuid(cs->uid)) {
    perror("setuid");
    exit(1);
  }

  execve("/bin/phantomjs", cs->argv, cs->envp);
  perror("execve");
  exit(1);
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

  if (chdir(cs->homedir)) {
    fprintf(stderr, "chdir: %s: %s\n", cs->homedir, strerror(errno));
    return;
  }

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
    execve("/bin/find", (char * const *)find_args, (char * const *)noenv);
    _exit(127);
  }

  if (waitpid(child, &status, 0) != child) {
    perror("waitpid");
    return;
  }
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    fprintf(stderr, "find exited unsuccessfully - status %04x\n", status);
    return;
  }
  if (rmdir(".qws"))
    perror("rmdir: .qws");
}

int main(int argc __attribute__((unused)), char **argv, char **envp)
{
  pid_t child;
  int status;
  child_state cs;
  memset(&cs, 0, sizeof cs);

  if (!select_homedir(&cs))
    return 1; /* no homedir available */

  if (!prepare_environment(&cs, argv, envp))
    return 1;

  fflush(0);
  child = fork();
  if (child == -1) {
    perror("fork");
    return 1;
  }
  if (child == 0)
    run_phantomjs(&cs);

  if (waitpid(child, &status, 0) != child) {
    perror("waitpid");
    return 1;
  }

  cleanup_homedir(&cs);

  if (WIFEXITED(status))
    return WEXITSTATUS(status);
  else {
    fprintf(stderr, "Child process killed by signal %d\n", WTERMSIG(status));
    return 1;
  }

  return 0;
}
