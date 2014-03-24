# Monitoring the progress of a multithreaded computation.
#
# Copyright © 2014 Zack Weinberg
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

import curses
import fcntl
import locale
import os
import queue
import select
import signal
import struct
import sys
import termios
import threading
import traceback

class Monitor:
    """Monitor wraps around a multithreaded program and provides it
       with a ncurses-based continual progress display.  Each thread
       of the program gets one line on the terminal, which it may
       update at will without interfering with other threads.  Monitor
       also fields signals that should interrupt or suspend the
       program and notifies the threads cleanly.

       Monitor is responsible for all terminal I/O, including signal
       management.  It uses threads internally and must be initialized
       before any other threads, as it manipulates the global signal
       mask and handlers.

       Like curses.wrapper, Monitor's constructor takes an argument
       which is a callable responsible for all your program logic.
       It will be called ON A NEW THREAD, passing the Monitor object
       itself, its own Thread object, and any additional arguments you
       provide.  On the initial thread, control does not leave
       Monitor.__init__ until program shutdown.  The optional "banner"
       argument to Monitor.__init__ allows you to set additional text
       in the top bar that tells the user how to stop the run.

       Monitor methods whose names begin with a single underscore MUST
       NOT be called from outside the Monitor object, or catastrophic
       thread-related failure may occur."""

    # Public API.
    def report_status(self, status):
        """Each worker thread should call this method at intervals to
           report its status.  The status may be any text you like.
           Writing a single ASCII NUL (that is, "\x00") clears the
           thread's status line."""
        self._tasks.put((self._STATUS,
                         self._line_indexes[threading.get_ident()],
                         status))

    def add_work_thread(self, worker_fn, *args, **kwargs):
        """The initial worker thread (the one that executes the
           callable passed to the constructor) may spin up additional
           workers by calling this function.  worker_fn is the callable
           to execute in the new thread; like the callable passed to the
           constructor, it receives two arguments, the Monitor and its own
           Thread object, plus any additional args passed to this function."""
        threading.Thread(target=self._work_thread_fn,
                         args=(worker_fn, args, kwargs)).start()

    def caller_is_only_active_thread(self):
        """True if the calling thread is the only active worker thread."""
        with self._counters_lock:
            return self._active_work_threads == 1

    def maybe_pause_or_stop(self):
        """Worker threads must call this method in between jobs; if
           the overall process is about to be suspended, it will block,
           and if the overall process is about to be terminated, it will
           exit the thread."""

        if not self._stop_event.is_set():
            # Raising SystemExit on a thread only terminates that
            # thread.  The worker wrapper routine will take care of
            # signaling an exit when all threads are done.
            self.report_status("\x00")
            raise SystemExit

        # Pausing doesn't map nicely onto any available primitive.
        # The desired logic is: at time 0, a controller thread raises
        # the "please pause soon" flag and blocks itself; when all the
        # worker threads have blocked, the controller becomes
        # unblocked; at some later time the controller releases the
        # workers.  The best available way to do this seems to be two
        # events and a counter.
        if not self._pause_event.is_set():
            with self._counters_lock:
                self._active_work_threads -= 1
                if not self._active_work_threads:
                    self._tasks.put((self._DONE, True))
            self.report_status("\x00")
            self._pause_event.wait()
            with self._counters_lock:
                self._active_work_threads += 1

    def __init__(self, main, *args, banner="", **kwargs):
        try:
            locale.setlocale(locale.LC_ALL, '')
            self._encoding = locale.getpreferredencoding()

            # Establish signal handling before doing anything else.
            for sig in self._SIGNALS:
                signal.signal(sig, self.dummy_signal_handler)
                signal.siginterrupt(sig, False)

            self._pid = os.getpid()
            try:
                self._sigpipe = os.pipe2(os.O_NONBLOCK|os.O_CLOEXEC)
            except AttributeError:
                self._sigpipe = os.pipe()
                fcntl.fcntl(self._sigpipe[0], fcntl.F_SETFL, os.O_NONBLOCK)
                fcntl.fcntl(self._sigpipe[1], fcntl.F_SETFL, os.O_NONBLOCK)
                fcntl.fcntl(self._sigpipe[0], fcntl.F_SETFD, fcntl.FD_CLOEXEC)
                fcntl.fcntl(self._sigpipe[1], fcntl.F_SETFD, fcntl.FD_CLOEXEC)
            self._old_wakeup = signal.set_wakeup_fd(self._sigpipe[1])

            self._tasks = queue.PriorityQueue()
            self._stop_event = threading.Event()
            self._pause_event = threading.Event()
            # pause_event and stop_event are active low
            self._stop_event.set()
            self._pause_event.set()

            self._output_thread = threading.current_thread()
            self._input_thread  = threading.Thread(
                target=self._input_thread_fn, daemon=True)
            self._mwork_thread  = threading.Thread(
                target=self._work_thread_fn, args=(main, args, kwargs))

            self._counters_lock = threading.Lock()
            self._n_work_threads = 0
            self._active_work_threads = 0
            self._worker_exceptions = {}

            # Terminal-related state.
            self._banner = banner
            self._addmsg = "Press ESC to stop."
            self._lines  = []
            self._line_attrs = []
            self._line_indexes = {}
            self._line_indexes_used = set()

            self._initscr_plus()

            # Release the hounds.
            self._input_thread.start()
            self._mwork_thread.start()
            self._output_thread_fn()

        # Control returns to this point only when we are tearing stuff
        # down.  Our internal threads can't practically be terminated,
        # but they are daemonized, and the others should have already
        # terminated.
        finally:
            curses.endwin()
            signal.set_wakeup_fd(self._old_wakeup)
            os.close(self._sigpipe[0])
            os.close(self._sigpipe[1])

            if self._worker_exceptions:
                for tid in sorted(self._worker_exceptions.keys()):
                    sys.stderr.write("Exception in thread {}:\n"
                                     .format(tid))
                    traceback.print_exception(*self._worker_exceptions[tid])
                    sys.stderr.write("\n")
                if sys.exc_info() == (None, None, None):
                    raise SystemExit(1)

    # Internal code numbers used for the output thread's work queue.
    # Lower numbers are higher message priorities (i.e. will be
    # delivered first).
    _DONE    = 4
    _STATUS  = 3
    _REDRAW  = 2
    _SUSPEND = 1
    _EXIT    = 0

    # Internal methods.

    # Stub signal handler.  All the signals are actually fielded via
    # the wakeup_fd mechanism.
    @staticmethod
    def dummy_signal_handler(*args, **kwargs):
        pass

    # See _input_thread_fn for how each signal is treated.
    # thanks everso, MacOS, for making me care what signals exist
    _SIGNALS = tuple(getattr(signal, s)
                     for s in dir(signal)
                     if (s.startswith("SIG") and not s.startswith("SIG_")
                         and s not in frozenset((
                # hopefully this is an exhaustive list of potential
                # kernel-generated synchronous and/or unblockable signals
                "SIGILL", "SIGTRAP", "SIGABRT", "SIGIOT", "SIGEMT", "SIGFPE",
                "SIGKILL", "SIGBUS", "SIGSEGV", "SIGSYS", "SIGSTOP", "SIGCONT",
                # signals that we can get but don't care about
                "SIGCLD", "SIGCHLD",
                # not actually signal numbers
                "SIGRTMIN", "SIGRTMAX"))))

    # Called in a couple of different places.
    def _initscr_plus(self):
        self._scr = curses.initscr()
        self._max_y, self._max_x = self._scr.getmaxyx()
        curses.noecho()
        curses.nonl()
        curses.cbreak()
        curses.typeahead(-1)
        curses.curs_set(0)
        # in case curses has other ideas
        fcntl.fcntl(0, fcntl.F_SETFL, os.O_NONBLOCK)

    def _input_thread_fn(self):

        def iter_avail_bytes(fd):
            try:
                while True:
                    yield os.read(fd, 1)
            except BlockingIOError:
                return

        def handle_input_char(ch):
            if ch == b'' or ch == b'\x1b' or ch == b'q' or ch == b'Q':
                self._tasks.put((self._EXIT, 0))
            elif ch == b'\f':
                self._tasks.put((self._REDRAW,))

        def handle_signal_char(ch):
            if len(ch) == 0:
                return
            sig = struct.unpack("B", ch)[0]
            if sig == signal.SIGWINCH:
                self._tasks.put((self._REDRAW,))
            elif (sig == signal.SIGTSTP or sig == signal.SIGTTIN or
                  sig == signal.SIGTTOU):
                self._tasks.put((self._SUSPEND, sig))
            else:
                self._tasks.put((self._EXIT, sig))

        handlers = { 0                : handle_input_char,
                     self._sigpipe[0] : handle_signal_char }

        poll = select.poll()
        poll.register(0, select.POLLIN)
        poll.register(self._sigpipe[0], select.POLLIN)

        while True:
            try:
                ready = poll.poll()
                for fd, _ in ready:
                    handler = handlers[fd]
                    for ch in iter_avail_bytes(fd):
                        handler(ch)
            except OSError:
                # We get -EBADF with some regularity during shutdown.
                break

    def _work_thread_fn(self, worker_fn, args, kwargs):
        thread = threading.current_thread()
        with self._counters_lock:
            self._n_work_threads += 1
            self._active_work_threads += 1
            i = 0
            while True:
                if i not in self._line_indexes_used:
                    self._line_indexes[thread.ident] = i
                    self._line_indexes_used.add(i)
                    break
                i += 1

        try:
            worker_fn(self, thread, *args, **kwargs)

        except Exception as e:
            self._worker_exceptions[thread.name] = sys.exc_info()
            msg = "*** Uncaught exception: " + \
                traceback.format_exception_only(type(e), e)[0][:-1]
            self.report_status(msg)
            self._tasks.put((self._EXIT, 0))

        finally:
            with self._counters_lock:
                i = self._line_indexes[thread.ident]
                del self._line_indexes[thread.ident]
                self._line_indexes_used.remove(i)
                self._n_work_threads -= 1
                self._active_work_threads -= 1

                if self._n_work_threads == 0:
                    self._tasks.put((self._DONE, False))

    # Subroutines of the main output loop.

    def _compute_banner_internal(self):
        """Compute the full text of the banner.  The banner is trimmed
           at the right if it + the exit message are too wide for the
           window, otherwise it is centered."""

        banner = self._banner
        w = self._max_x

        if banner == "":
            msg = self._addmsg
        else:
            msg = ". " + self._addmsg

        n = len(msg)
        if n > w:
            return msg[n-w:]

        if banner != "":
            n = len(banner) + len(msg)
            if n > w:
                shortfall = n - w
                if len(banner) <= 2:
                    return banner[:-shortfall] + msg
                elif w - len(msg) <= 2:
                    return "."*(w - len(msg)) + msg
                else:
                    shortfall += 2
                    return banner[:-shortfall] + ".." + msg

            msg = banner + msg

        space = w - n
        if space == 0:
            return msg

        if space % 2 == 0:
            pad = " " * (space // 2)
            full_msg = pad + msg + pad
        else:
            pad1 = " " * (space // 2)
            pad2 = pad1 + " "
            full_msg = pad1 + msg + pad2

        return full_msg

    def _compute_banner(self):
        return self._compute_banner_internal().encode(self._encoding)

    def _do_status(self, idx, text):
        while idx >= len(self._lines):
            self._lines.append("")
            self._line_attrs.append(curses.A_NORMAL)

        if text == "\x00":
            self._line_attrs[idx] = curses.A_NORMAL
        else:
            self._lines[idx] = text.encode(self._encoding)

        y = (self._max_y - 1) - idx
        if y < 1: return # the top line is reserved for the banner
        self._scr.addnstr(y, 0,
                          self._lines[idx], self._max_x-1,
                          self._line_attrs[idx])
        self._scr.clrtoeol()
        self._scr.refresh()

    def _do_redraw(self):
        # Unconditionally query the OS for the size of the window whenever
        # we need to do a complete redraw.
        height, width = struct.unpack("hhhh",
                                      fcntl.ioctl(0, termios.TIOCGWINSZ,
                                                  b"\000"*8))[0:2]
        self._max_y = height
        self._max_x = width
        curses.resizeterm(height, width)

        self._scr.clear()
        self._scr.addstr(0, 0, self._compute_banner(), curses.A_REVERSE)
        for y in range(1, self._max_y):
            idx = (self._max_y - 1) - y
            if idx < len(self._lines):
                self._scr.addnstr(y, 0, self._lines[idx], self._max_x-1,
                                  self._line_attrs[idx])
        self._scr.refresh()

    def _flag_all_lines(self):
        for i in range(len(self._line_attrs)):
            self._line_attrs[i] = curses.A_BOLD
        self._do_redraw()

    def _do_suspend(self, signo, old_addmsg):
        def drain_input(fd):
            try:
                while len(os.read(fd, 1024)) > 0:
                    pass
            except BlockingIOError:
                return

        curses.endwin()
        signal.signal(signo, signal.SIG_DFL)
        os.kill(self._pid, signo)

        signal.signal(signo, self.dummy_signal_handler)
        signal.siginterrupt(signo, False)
        self._initscr_plus()
        drain_input(0)
        drain_input(self._sigpipe[0])

        self._pause_event.set()
        self._addmsg = old_addmsg
        self._do_redraw()

    def _do_exit(self, signo):
        if signo == 0 or signo == signal.SIGINT:
            return

        curses.endwin()
        signal.pthread_sigmask(signal.SIG_UNBLOCK, [signo])
        signal.signal(signo, signal.SIG_DFL)
        os.kill(self._pid, signo)

    def _output_thread_fn(self):
        self._scr.clear()
        self._scr.addstr(0, 0, self._compute_banner(), curses.A_REVERSE)
        self._scr.refresh()
        old_addmsg = None
        exit_signal = 0

        while True:
            try:
                task = self._tasks.get()
                if task[0] == self._STATUS:
                    self._do_status(task[1], task[2])

                elif task[0] == self._REDRAW:
                    self._do_redraw()

                elif task[0] == self._SUSPEND:
                    exit_signal = task[1]
                    self._pause_event.clear()

                    # a _DONE message will be posted to the queue as soon
                    # as all worker threads respond to the pause event
                    old_addmsg = self._addmsg
                    self._addmsg = "Pausing."
                    self._flag_all_lines()

                elif task[0] == self._EXIT:
                    exit_signal = task[1]
                    self._stop_event.clear()
                    # a _DONE message will be posted to the queue as soon
                    # as all worker threads respond to the stop event
                    if exit_signal:
                        self._addmsg = ("Shutting down (signal {})."
                                        .format(exit_signal))
                    else:
                        self._addmsg = "Shutting down."
                    self._flag_all_lines()

                elif task[0] == self._DONE:
                    if task[1]:
                        self._do_suspend(exit_signal, old_addmsg)
                        exit_signal = 0
                        old_addmsg = None
                    else:
                        self._do_exit(exit_signal)
                        return

                else:
                    raise RuntimeError("invalid task: " + repr(task))

            # Normally, no exceptions whatsoever may escape this function.
            # Allow them to do so if we are already trying to stop, or we
            # might get stuck forever.
            except BaseException as e:
                if not self._stop_event.is_set():
                    raise

                self._stop_event.clear()
                self._addmsg = ("*** {}:{} *** Crashing."
                                .format(type(e).__name__, str(e)))
                self._do_redraw()
