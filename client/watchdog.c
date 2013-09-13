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

/* The most exotic features this program requires are pselect(), which
   is in POSIX.1-2001, and strsignal(), which is only in POSIX.1-2008 (!) */
#define _POSIX_C_SOURCE 200809L

#include <sys/types.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
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

#if   !defined EWOULDBLOCK && !defined EAGAIN
#error "Neither EAGAIN nor EWOULDBLOCK is defined"
#elif  defined EWOULDBLOCK && !defined EAGAIN
#define EAGAIN EWOULDBLOCK
#elif !defined EWOULDBLOCK &&  defined EAGAIN
#define EWOULDBLOCK EAGAIN
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

static sigset_t original_sigmask;

/* Called on the child side of a fork().  Does not return under any
   circumstances.  Indicates low-level failures by writing error
   messages to the 'output_fd' and exiting specially.  Relies on
   parent taking care to set every fd close-on-exec, since Linux still
   doesn't have closefrom(), sigh.  */
static NORETURN
child_startup(int devnull_fd, int output_fd, char **argv)
{
  if (dup2(devnull_fd, 0) != 0)
    child_startup_err(output_fd, "<child>: setting stdin", 0);

  if (dup2(output_fd, 1) != 1)
    child_startup_err(output_fd, "<child>: setting stdout", 0);

  if (dup2(output_fd, 2) != 2)
    child_startup_err(output_fd, "<child>: setting stderr", 0);

  if (setpgid(0, 0))
    child_startup_err(output_fd, "<child>: setpgid", 0);

  if (sigprocmask(SIG_SETMASK, &original_sigmask, 0))
    child_startup_err(output_fd, "<child>: reset sigmask", 0);

  execvp(argv[0], argv);
  child_startup_err(output_fd, argv[0], "execvp");
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
  struct child_state rv = { -1, -1 };

  /* 'opipe' will feed output from the child to the parent.  We only
     want the read end of the output pipe to be nonblocking, but both
     ends should be close-on-exec. */
  if (pipe(opipe)) {
    perror("creating output pipe");
    return rv;
  }
  flags = fcntl(opipe[0], F_GETFL);
  if (flags == -1 ||
      fcntl(opipe[0], F_SETFL, flags | O_NONBLOCK) == -1 ||
      fcntl(opipe[0], F_SETFD, FD_CLOEXEC) == -1 ||
      fcntl(opipe[1], F_SETFD, FD_CLOEXEC) == -1) {
    perror("setting output pipe to nonblocking + close-on-exec");
    close(opipe[0]);
    close(opipe[1]);
    return rv;
  }

  /* 'devnull_fd' will be the child's stdin. */
  devnull_fd = open("/dev/null", O_RDONLY);
  if (devnull_fd == -1 ||
      fcntl(devnull_fd, F_SETFD, FD_CLOEXEC) == -1) {
    perror("/dev/null");
    close(opipe[0]);
    close(opipe[1]);
    return rv;
  }

  /* 'spipe' is used to synchronize parent and child after the fork. */
  if (pipe(spipe) ||
      fcntl(spipe[0], F_SETFD, FD_CLOEXEC) == -1 ||
      fcntl(spipe[1], F_SETFD, FD_CLOEXEC) == -1) {
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

static void
report_exit(pid_t exited_child, pid_t expected_child, int status)
{
  /* special case: don't print anything if this is the expected child and
     it exited with code 127, because that will already have been covered
     by child_startup_err */
  if (exited_child == expected_child && WIFEXITED(status) &&
      WEXITSTATUS(status) == 127)
    return;

  if (exited_child == expected_child)
    fputs("monitored child ", stderr);
  else
    fprintf(stderr, "unexpected child %lu ", (unsigned long)exited_child);

  if (status == 0)
    fputs("exited successfully\n", stderr);
  else if (WIFEXITED(status))
    fprintf(stderr, "exited unsuccessfully (code %u)\n",
            (unsigned)WEXITSTATUS(status));
  else if (WIFSIGNALED(status))
    fprintf(stderr, "killed by signal: %s%s\n",
            strsignal(WTERMSIG(status)),
#ifdef WCOREDUMP
            WCOREDUMP(status) ? " (core dumped)" :
#endif
            "");
  else
    fprintf(stderr, "produced un-decodable wait status %04x\n",
            (unsigned) status);
}

/* Signal handler used in conjunction with pselect() in the main loop
   below. */
static volatile sig_atomic_t
  got_SIGHUP,
  got_SIGINT,
  got_SIGQUIT,
  got_SIGTERM,
  got_SIGXCPU,
  got_SIGCHLD;

static void
interrupt_signal(int signal)
{
  switch (signal) {
  case SIGHUP:  got_SIGHUP  = 1; break;
  case SIGINT:  got_SIGINT  = 1; break;
  case SIGQUIT: got_SIGQUIT = 1; break;
  case SIGTERM: got_SIGTERM = 1; break;
  case SIGXCPU: got_SIGXCPU = 1; break;
  case SIGCHLD: got_SIGCHLD = 1; break;
  default:
    abort();
  }
}

/* Terminate both this process and the monitored process group with
   the specified fatal signal.  Does not return. */
static NORETURN
exit_on_signal(int signo, const struct child_state *child)
{
  sigset_t unblocker;

  if (child->pid > 1) {
    if (kill(-child->pid, signo))
      perror("killpg");
  } else {
    if (child->pid != -1) {
      fprintf(stderr, "warning: cannot kill pgrp %ld\n", (long)child->pid);
    }
  }

  /* Flush output, since we're about to force an abnormal termination. */
  fflush(0);

  /* Before raising the signal, we have to reset it to default
     behavior and unblock it, or nothing will happen.  If any of the
     following system calls fail, there's no use reporting the error,
     just fall through to the abort(). */
  sigemptyset(&unblocker);
  sigaddset(&unblocker, signo);
  signal(signo, SIG_DFL);
  sigprocmask(SIG_UNBLOCK, &unblocker, 0);
  raise(signo);

  /* should not get here */
  abort();
}

/* Copy all output from 'fd' to our stdout.  Record the last six
   characters of the cumulative output in 'donebuf'. */
static void
process_output(int fd, char donebuf[6])
{
  char block[PIPE_BUF];
  ssize_t count;
  char *p;
  size_t n;
  for (;;) {
    count = read(fd, block, sizeof block);
    if (count == 0)
      break; /* EOF */
    if (count == -1) {
      if (errno == EINTR)
        continue; /* this shouldn't happen but let's be defensive */

      /* Report the error, unless it's "would block", in which case
         we're done */
      if (errno != EAGAIN && errno != EWOULDBLOCK)
        perror("read");
      break;
    }

    for (p = block; p < block + count; ) {
      n = fwrite(p, 1, count - (p - block), stdout);
      if (n == 0) {
        perror("fwrite");
        break;
      }
      p += n;
    }
    fflush(stdout);

    if (count >= 6)
      memcpy(donebuf, block + count - 6, 6);
    else {
      /* When 0 < count < 6, we want to slide the existing contents of
         'donebuf' down by 'count' positions and copy the entire block
         into place after them. Example:

         count = 2: ------v
         before     [x][x][1][2][3][4]
         after      [1][2][3][4][a][b]  */
      memmove(donebuf, donebuf + count, 6 - count);
      memcpy(donebuf + (6 - count), block, count);
    }
  }
}

int
main(int argc, char **argv)
{
  struct sigaction sa;
  sigset_t all_blocked, handled_unblocked;
  struct child_state child;
  fd_set readfds;
  int ready;
  struct timespec timeout, zero_timeout;
  int retries;
  int child_died, child_done;
  char donebuf[7];

  if (argc < 2) {
    fprintf(stderr, "usage: %s program-to-monitor args...\n", argv[0]);
    return 2;
  }

  /* Establish signal handlers before doing anything else. */
  sigfillset(&all_blocked);
  sigdelset(&all_blocked, SIGABRT);
  sigdelset(&all_blocked, SIGBUS);
  sigdelset(&all_blocked, SIGFPE);
  sigdelset(&all_blocked, SIGILL);
  sigdelset(&all_blocked, SIGIOT);
  sigdelset(&all_blocked, SIGSEGV);
  sigdelset(&all_blocked, SIGSYS);
  sigdelset(&all_blocked, SIGTRAP);
  if (sigprocmask(SIG_SETMASK, &all_blocked, &original_sigmask)) {
    perror("sigprocmask");
    return 1;
  }

  memset(&sa, 0, sizeof sa);
  memcpy(&sa.sa_mask, &all_blocked, sizeof(sigset_t));
  sa.sa_handler = interrupt_signal;
  sa.sa_flags   = 0; /* deliberate non-use of SA_RESTART; we want these
                        signals to interrupt pselect() */

  memcpy(&handled_unblocked, &all_blocked, sizeof(sigset_t));
  sigdelset(&handled_unblocked, SIGCHLD);
  if (sigaction(SIGCHLD, &sa, 0)) {
    perror("sigaction(SIGCHLD)");
    return 1;
  }

  /* These signals might have been ignored at a higher level, in which
     case we need to keep it that way */
#define maybe_establish_signal_handler(signo) do {      \
    struct sigaction osa;                               \
    if (sigaction(signo, &sa, &osa)) {                  \
      perror("sigaction(" #signo ")");                  \
      return 1;                                         \
    }                                                   \
    if (osa.sa_handler == SIG_IGN) {                    \
      if (sigaction(signo, &osa, 0)) {                  \
        perror("sigaction(" #signo ")");                \
        return 1;                                       \
      }                                                 \
    } else {                                            \
      sigdelset(&handled_unblocked, signo);             \
    }                                                   \
  } while (0)

  maybe_establish_signal_handler(SIGHUP);
  maybe_establish_signal_handler(SIGINT);
  maybe_establish_signal_handler(SIGQUIT);
  maybe_establish_signal_handler(SIGTERM);
  maybe_establish_signal_handler(SIGXCPU);

  /* Unlike select, pselect is guaranteed not to modify 'timeout'. */
  timeout.tv_sec = 5 * 60;
  timeout.tv_nsec = 0;
  zero_timeout.tv_sec = 0;
  zero_timeout.tv_nsec = 0;
  retries = 0;

  do {
    memset(donebuf, 0, sizeof donebuf);
    child_died = 0;
    child_done = 0;

    if (retries >= 10) {
      fputs("failed to start child 10 times; giving up\n", stderr);
      return 1;
    }
    child = child_spawn(argv+1);
    if (child.pid == -1) {
      retries++;
      continue;
    }

    do {
      FD_ZERO(&readfds);
      FD_SET(child.out_fd, &readfds);
      got_SIGHUP  = 0;
      got_SIGINT  = 0;
      got_SIGQUIT = 0;
      got_SIGTERM = 0;
      got_SIGXCPU = 0;
      got_SIGCHLD = 0;

      /* Some OSes' versions of pselect() will not unblock signals if
         any file descriptors are ready upon entry, even if some of
         the signals that would be unblocked are already pending.
         And a pipe at EOF (i.e. all writers have closed their end)
         is always ready to read.  Therefore, if we don't take special
         care, we might get stuck in an infinite loop between
         pselect() and read(), with signals never getting delivered --
         including the SIGCHLD we are waiting for, before closing our
         end of the pipe!  The workaround is to do two pselect calls,
         the first of which is strictly a poll for signals. */
      ready = pselect(0, 0, 0, 0, &zero_timeout, &handled_unblocked);
      if (ready == 0)
        ready = pselect(child.out_fd + 1, &readfds, 0, 0,
                        &timeout, &handled_unblocked);
      if (ready == -1 && errno != EINTR)
        perror("pselect");

      /* Order of operations here is critical.  First read everything
         we can from the pipe; then check for SIGCHLD; then check for
         other signals. */
      if (ready > 0)
        /* We know by construction that there's only one fd that can
           be ready. */
        process_output(child.out_fd, donebuf);

      if (got_SIGCHLD)
        /* It is necessary to loop through waitpid() until it either
           returns 0 or fails with ECHILD, otherwise we may miss
           exits.  This should theoretically not be an issue since
           there *should* only be one child to wait for at any time,
           but let's be careful anyway. */
        for (;;) {
          pid_t exited_child;
          int status;
          exited_child = waitpid((pid_t)-1, &status, WNOHANG);
          if (exited_child == 0)
            break;
          if (exited_child == -1) {
            if (errno != ECHILD)
              perror("waitpid");
            break;
          }
          report_exit(exited_child, child.pid, status);
          if (exited_child == child.pid) {
            /* Try one last time to read remaining output from the pipe. */
            process_output(child.out_fd, donebuf);
            close(child.out_fd);
            child_died = 1;
            if (status == 0)
              child_done = !strcmp(donebuf, "\nDONE\n");
            else {
              child_done = 0;
              if (WIFEXITED(status) && WEXITSTATUS(status) == 127)
                /* startup failure after fork() */
                retries++;
            }

            /* Zot the process group, in case there's anything still
               hanging around. */
            if (child.pid > 1)
              kill(-child.pid, SIGTERM);

            child.out_fd = -1;
            child.pid = -1;
          }
        }

      /* If we have received a signal that should cause us to exit,
         pass the signal along to the entire child process group if
         the child is running, then unblock and reraise that signal. */
      if (got_SIGQUIT)
        exit_on_signal(SIGQUIT, &child);
      if (got_SIGXCPU)
        exit_on_signal(SIGXCPU, &child);
      if (got_SIGINT)
        exit_on_signal(SIGINT, &child);
      if (got_SIGHUP)
        exit_on_signal(SIGHUP, &child);
      if (got_SIGTERM)
        exit_on_signal(SIGTERM, &child);

      /* If we timed out waiting for I/O and the child process is
         still running, kill it and loop; the subsequent iteration
         will detect termination and restart it.  */
      if (ready == 0 && !child_died)
        kill(-child.pid, SIGTERM);

    } while (!child_died);
  } while (!child_done);

  return 0;
}
