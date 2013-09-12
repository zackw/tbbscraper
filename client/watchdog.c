/*
 * Usage: watchdog worker args ...
 * Runs 'worker' and all its subprocesses in a dedicated process group.
 * If 'worker' exits, or if it produces no output (on either stdout or
 * stderr) for five minutes, the entire process group is killed off and
 * restarted.
 *
 * We consider 'worker' to be done with its work, and do not restart
 * it, if it exits successfully *and* the last six characters of its
 * output are exactly "\nDONE\n".
 */

#define _GNU_SOURCE

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#ifdef __GNUC__
#define NORETURN void __attribute__((noreturn))
#else
#define NORETURN void
#endif

/* Called on the child side of fork(); report a system error and exit
   specially (this will cause the parent not to retry). Does not
   return. Deliberately ignores all errors. */
static NORETURN
child_startup_err(int fd, const char *msg1, const char *msg2)
{
#define IGNORE_ERRORS(expr) if (expr) do {} while (0)
  const char *err = strerror(errno);
  IGNORE_ERRORS(write(fd, msg1, strlen(msg1)));
  if (msg2) {
    IGNORE_ERRORS(write(fd, ": ", 2));
    IGNORE_ERRORS(write(fd, msg2, strlen(msg2)));
  }
  IGNORE_ERRORS(write(fd, ": ", 2));
  IGNORE_ERRORS(write(fd, err, strlen(err)));
  IGNORE_ERRORS(write(fd, "\n", 1));
  _exit(127);
#undef IGNORE_ERRORS
}

/* Called on the child side of a fork().  Does not return under any
   circumstances.  Indicates low-level failures by writing error
   messages to the 'output_fd' and exiting specially.  Relies on
   parent taking care to set every fd close-on-exec, since Linux still
   doesn't have closefrom(), sigh.  */
static NORETURN
child_startup(int devnull_fd, int output_fd, char **argv)
{
  if (dup3(devnull_fd, 0, 0) != 0)
    child_startup_err(output_fd, "<child>: setting stdin", 0);

  if (dup3(output_fd, 1, 0) != 1)
    child_startup_err(output_fd, "<child>: setting stdout", 0);

  if (dup3(output_fd, 2, 0) != 2)
    child_startup_err(output_fd, "<child>: setting stderr", 0);

  if (setpgid(0, 0))
    child_startup_err(output_fd, "<child>: setpgid", 0);

  execvp(argv[0], argv);
  child_startup_err(output_fd, argv[0], "execvp")
}

struct child_state
{
  pid_t pid;
  int out_fd;
};

/* Parent-side child preparation work.  Returns the child PID and the
   read end of a pipe connected to the child's stdout+stderr, or { -1, -1 }
   on failure. */
static struct child_state
child_spawn(char **argv)
{
  int opipe[2], spipe[2];
  int devnull_fd;
  int flags;
  char syncbuf[1];
  struct child_state rv = { -1, -1 }

  /* 'opipe' will feed output from the child to the parent.  We only
     want the read end of the output pipe to be nonblocking, so we
     don't use O_NONBLOCK in pipe2(). */
  if (pipe2(opipe, O_CLOEXEC)) {
    perror("creating output pipe");
    return rv;
  }
  flags = fcntl(opipe[0], F_GETFL);
  if (flags == -1 ||
      fcntl(opipe[0], F_SETFL, flags | O_NONBLOCK) == -1) {
    perror("setting output pipe to nonblocking");
    close(opipe[0]);
    close(opipe[1]);
    return rv;
  }

  /* 'devnull_fd' will be the child's stdin. */
  devnull_fd = open("/dev/null", O_RDONLY|O_CLOEXEC);
  if (devnull_fd == -1) {
    perror("/dev/null");
    close(opipe[0]);
    close(opipe[1]);
    return rv;
  }

  /* 'spipe' is used to synchronize parent and child after the fork. */
  if (pipe2(spipe, O_CLOEXEC)) {
    perror("creating synchronization pipe");
    close(devnull_fd);
    close(opipe[0]);
    close(opipe[1]);
    return rv;
  }

  rv.pid = fork();
  if (rv.pid == -1) {
    perror("fork");
    close(spipe[0]);
    close(spipe[1]);
    close(devnull_fd);
    close(opipe[0]);
    close(opipe[1]);
    return rv;
  }

  if (rv.pid == 0)
    child_startup(devnull_fd, opipe[1], argv); /* does not return */

  /* If we get here, we are the parent, and these file descriptors are
     no longer required.  */
  close(spipe[1]);
  close(devnull_fd);
  close(opipe[1]);

  /* Wait for the child to complete its setup and call execve() or
     _exit().  In either case, the read end of 'spipe' will be
     automatically closed in the child, and this read() call will
     return.  */
  if (read(spipe[0], syncbuf, 1) != 0)
    perror("synchronization pipe read");
  close(spipe[0]);

  rv.out_fd = opipe[0];
  return rv;
}

/* Signal handler used in conjunction with pselect() in the main loop
   below. */
static volatile sig_atomic_t last_signal = 0;
static void
interrupt_signal(int signal)
{
  last_signal = signal;
}

int
main(int argc, char **argv)
{
  struct sigaction sa;
  sigset_t all_blocked, handled_unblocked;
  struct child_state child;
  fd_set readfds;
  int ready;
  struct timespec timeout;
  int retries;

  if (argc < 2) {
    fprintf(stderr, "usage: %s program-to-monitor args...\n", argv[0]);
    return 2;
  }

  /* Establish signal handlers before doing anything else. */
  sigfillset(&all_blocked);
  sigdelset(&all_blocked, SIGBUS);
  sigdelset(&all_blocked, SIGFPE);
  sigdelset(&all_blocked, SIGILL);
  sigdelset(&all_blocked, SIGSEGV);

  memcpy(&handled_unblocked, &all_blocked, sizeof(sigset_t));
  sigdelset(&handled_unblocked, SIGCHLD);
  sigdelset(&handled_unblocked, SIGINT);
  sigdelset(&handled_unblocked, SIGQUIT);
  sigdelset(&handled_unblocked, SIGTERM);

  memset(&sa, 0, sizeof sa);
  memcpy(&sa.sa_mask, &all_blocked, sizeof(sigset_t));
  sa.sa_handler = interrupt_signal;
  sa.sa_flags   = 0; /* deliberate non-use of SA_RESTART; we want these
                        signals to interrupt pselect() */

  if (sigprocmask(SIG_SETMASK, &all_blocked, 0)) {
    perror("sigprocmask");
    return 1;
  }
  if (sigaction(SIGCHLD, &sa, 0) ||
      sigaction(SIGINT,  &sa, 0) ||
      sigaction(SIGQUIT, &sa, 0) ||
      sigaction(SIGTERM, &sa, 0)) {
    perror("sigaction");
    return 1;
  }

  /* Unlike select, pselect is guaranteed not to modify 'timeout'. */
  timeout.tv_sec = 5 * 60;
  timeout.tv_usec = 0;
  retries = 0;

  for (;;) {
    child = child_spawn(argv+1);
    if (child.pid == -1) {
      retries++;
      if (retries > 10) {
        fputs("failed to start child 10 times; giving up\n", stderr);
        return 1;
      }
      continue;
    }

    for (;;) {
      FD_ZERO(&readfds);
      FD_SET(child.out_fd, &readfds);
      ready = pselect(&readfds, 0, 0, &timeout, &handled_unblocked);

      /* tricky part goes here */
    }
  }
}
