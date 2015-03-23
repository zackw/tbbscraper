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
import datetime
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

    def set_status_prefix(self, prefix, thread=None):
        """Each worker thread may set a prefix to be prepended to each of
           its status reports.  The thread= argument can override
           which thread the message is deemed to be from."""
        if thread is None:
            thread = threading.get_ident()
        self._last_prefix[thread] = prefix
        self._tasks.put((self._STATUS, thread))

    def report_status(self, status, thread=None):
        """Each worker thread should call this method at intervals to
           report its status.  The status may be any text you like.
           The thread= argument can override which thread the message
           is deemed to be from.
        """
        if thread is None:
            thread = threading.get_ident()
        self._last_status[thread] = status
        self._tasks.put((self._STATUS, thread))

    def report_error(self, status, thread=None):
        """Like report_status, but also writes the STATUS to the global
           error log."""
        if thread is None:
            thread = threading.get_ident()
        self.report_status(status, thread)

        prefix = self._last_prefix[thread]
        if prefix:
            prefix = "{} {} ({}): ".format(
                datetime.datetime.now().isoformat(sep=' '),
                thread, prefix)
        else:
            prefix = "{} {}: ".format(
                datetime.datetime.now().isoformat(sep=' '),
                thread)
        self._error_log.write(prefix + status + "\n")
        self._error_log.flush()

    def report_exception(self, einfo=None, thread=None):
        """Like report_error, but the status is taken from 'einfo' and a
           complete traceback is written to the global error log."""
        if einfo is None:
            einfo = sys.exc_info()
        if thread is None:
            thread = threading.get_ident()
        status = traceback.format_exception_only(einfo[0], einfo[1])[0][:-1]

        self.report_error(status, thread)
        for chunk in traceback.format_exception(*einfo):
            for line in chunk.splitlines():
                self._error_log.write("| " + line + "\n")
        self._error_log.flush()

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

    def maybe_pause_or_stop(self, before_stopping=None):
        """Worker threads must call this method in between jobs; if
           the overall process is about to be suspended, it will block,
           and if the overall process is about to be terminated, it will
           exit the thread.  The optional callback allows workers to
           carry out any cleanup actions that may be necessary before
           they suspend or stop."""
        if self._stop_event.is_set():
            if before_stopping is not None: before_stopping()
            self._do_pause_or_stop()

    def idle(self, timeout, before_stopping=None):
        """Worker threads should call this method if they have nothing to
           do for some fixed amount of time.  It behaves as time.sleep(),
           but responds immediately to a pause or stop signaled during the
           idle period."""
        if self._stop_event.wait(timeout):
            if before_stopping is not None: before_stopping()
            self._do_pause_or_stop()

    def register_event_queue(self, queue, desired_stop_message):
        """Threads that take work from an event queue, and may block
           indefinitely on that queue, should register it by calling
           this function; all such queues will get posted a special
           message whenever the thread needs to call
           maybe_pause_or_stop() in a timely fashion.

           The special message is simply whatever the application
           provides as the desired_stop_message argument.  This is so
           the message can be made to conform to whatever scheme is in
           use for other messages.
        """
        self._worker_event_queues[threading.get_ident()] = \
            ('q', queue, desired_stop_message)

    def register_event_pipe(self, pipe, desired_stop_message):
        """Same as register_event_queue, but the thread takes work from
           one or more OS-level pipes.  PIPE is the write fd of one of
           these pipes, and the stop message must be acceptable to os.write.
        """
        self._worker_event_queues[threading.get_ident()] = \
            ('p', pipe, desired_stop_message)

    def __init__(self, main, *args, banner="", error_log="error-log",
                 **kwargs):
        try:
            locale.setlocale(locale.LC_ALL, '')
            self._encoding = locale.getpreferredencoding()

            self.open_error_log(error_log)

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
            self._resume_event = threading.Event()
            # pause_event is active low
            self._resume_event.set()

            self._output_thread = threading.current_thread()
            self._input_thread  = threading.Thread(
                target=self._input_thread_fn, daemon=True)
            self._mwork_thread  = threading.Thread(
                target=self._work_thread_fn, args=(main, args, kwargs))

            self._counters_lock = threading.Lock()
            self._n_work_threads = 0
            self._active_work_threads = 0
            self._worker_event_queues = {}
            self._worker_exceptions = {}

            # Terminal-related state.
            self._banner = banner
            self._addmsg = "Press ESC to stop."
            self._lines  = []
            self._line_attrs = []
            self._line_indexes = {}
            self._line_indexes_used = 0
            self._last_prefix = {}
            self._last_status = {}

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

    def open_error_log(self, error_log):
        error_log = error_log + "." + datetime.date.today().isoformat()
        suffix = ""
        n = 0
        while True:
            try:
                self._error_log = open(error_log + suffix + ".txt", "xt")
                return
            except FileExistsError:
                n += 1
                if n == 1000:
                    raise
                suffix = ".{}".format(n)

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
            idx = self._line_indexes_used
            self._line_indexes[thread.ident] = idx
            while idx >= len(self._lines):
                self._lines.append(b"")
                self._line_attrs.append(curses.A_NORMAL)

            self._line_indexes_used += 1
            self._n_work_threads += 1
            self._active_work_threads += 1

        self._last_prefix[thread.ident] = ""
        self._last_status[thread.ident] = ""

        try:
            worker_fn(self, thread, *args, **kwargs)

        except Exception as e:
            self._worker_exceptions[thread.name] = sys.exc_info()
            msg = "*** Uncaught exception: " + \
                traceback.format_exception_only(type(e), e)[0][:-1]
            self.report_status(msg)
            self._tasks.put((self._EXIT, 0))

        finally:
            if thread.ident in self._worker_event_queues:
                del self._worker_event_queues[thread.ident]
            with self._counters_lock:
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

    def _redraw_line(self, idx):
        y = (self._max_y - 1) - idx
        if y < 1: return # the top line is reserved for the banner
        self._scr.addnstr(y, 0,
                          self._lines[idx], self._max_x-1,
                          self._line_attrs[idx])
        self._scr.clrtoeol()
        self._scr.refresh()

    def _do_status(self, thread):
        idx = self._line_indexes[thread]
        prefix = self._last_prefix[thread]
        status = self._last_status[thread]

        if not prefix:
            text = status
        elif prefix[-1] == ' ':
            text = prefix + status
        else:
            text = prefix + ": " + status

        self._lines[idx] = text.encode(self._encoding)
        self._redraw_line(idx)

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

    def _unflag_thread(self):
        thread = threading.get_ident()
        idx = self._line_indexes[thread]
        self._line_attrs[idx] = curses.A_NORMAL
        self._tasks.put((self._STATUS, thread))

    def _do_pause_or_stop(self):
        # Note: this function is called on a worker thread, not the
        # display thread.

        if not self._resume_event.is_set():
            # If the stop event is set and the resume event is clear,
            # that means pause.  Pausing doesn't map nicely onto any
            # available primitive.  The desired logic is: at time 0, a
            # controller thread raises the "please pause soon" flag
            # and blocks itself; when all the worker threads have
            # blocked, the controller becomes unblocked; at some later
            # time the controller releases the workers.  The best
            # available way to do this seems to be two events and a
            # counter.
            with self._counters_lock:
                self._active_work_threads -= 1
                if not self._active_work_threads:
                    self._tasks.put((self._DONE, True))
            self._unflag_thread()
            self._resume_event.wait()
            with self._counters_lock:
                self._active_work_threads += 1

        else:
            # If both the stop and resume events are set, that means stop.
            # Raising SystemExit on a thread only terminates that
            # thread.  The worker wrapper routine will take care of
            # signaling an exit when all threads are done.
            self._unflag_thread()
            raise SystemExit

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

        self._resume_event.set()
        self._addmsg = old_addmsg
        self._do_redraw()

    def _do_exit(self, signo):
        if signo == 0 or signo == signal.SIGINT:
            return

        curses.endwin()
        signal.pthread_sigmask(signal.SIG_UNBLOCK, [signo])
        signal.signal(signo, signal.SIG_DFL)
        os.kill(self._pid, signo)

    def _broadcast_stop(self):
        for t, q, m in self._worker_event_queues.values():
            if t == 'q':
                q.put(m)
            else:
                assert t == 'p'
                os.write(fd, m)
        self._stop_event.set()

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
                    self._resume_event.clear()
                    self._broadcast_stop()

                    # a _DONE message will be posted to the queue as soon
                    # as all worker threads respond to the pause event
                    old_addmsg = self._addmsg
                    self._addmsg = "Pausing."
                    self._flag_all_lines()

                elif task[0] == self._EXIT:
                    exit_signal = task[1]
                    self._broadcast_stop()
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
                if self._stop_event.is_set():
                    raise

                self._broadcast_stop()
                self._addmsg = ("*** {}:{} *** Crashing."
                                .format(type(e).__name__, str(e)))
                self._do_redraw()

class Worker:
    """Skeleton for a work thread that takes messages from a dispatcher.
       Subclasses must implement the process_batch method.  Whatever
       process_batch returns will be passed back to the dispatcher.
       (If appropriate, process_batch may choose to call
       self._disp.complete_batch itself; in that case it should return
       None.) If process_batch throws an exception, that will also be
       reported to the dispatcher.

       The dispatcher, which must be the 'disp' object passed to
       __init__, can queue work on a Worker by calling its queue_batch
       method.  It can direct a Worker to terminate (after completing
       any previously queued work) by calling its 'finished' method.

       The dispatcher must itself implement three methods:
       complete_batch(worker, results), fail_batch(worker, exc_info),
       and drop_worker(worker), where 'worker' is the Worker object,
       'results' is whatever process_batch returned, and 'exc_info' is
       sys.exc_info() for the exception that process_batch threw.  The
       dispatcher is responsible for keeping track of the correlation
       among workers, batches, and results or failures.

       If a batch might take a very long time to process,
       process_batch should call self.is_interrupted() at appropriate
       intervals, and return as soon as possible if it returns True.

       The idle property is True if this worker currently has nothing to
       do, and False if it is currently processing a batch.

       The _disp property holds the dispatcher object passed to the
       constructor.

       The _mon property is guaranteed to be a Monitor object.
       process_batch may (indeed, should) make use of
       self._mon.report_status and self._mon.set_status_prefix as it
       sees fit.

       The _idle_prefix property, which should be initialized in a
       subclass's __init__ method, will be passed to
       self._mon.set_status_prefix whenever the worker goes idle.
       (It does not need to contain the string "idle".)

       All other properties defined in this base class should be
       considered private.

    """

    def __init__(self, disp):
        self._disp = disp
        self._mon = None
        self._batch_queue = queue.PriorityQueue()
        self._serializer = 0
        self._idle_prefix = ""
        self.idle = True

    # batch queue message types/priorities
    _INTERRUPT = 1
    _BATCH     = 2
    _DONE      = 3

    # dispatcher-to-worker API
    def queue_batch(self, *args, **kwargs):
        # PriorityQueue messages must be totally ordered.  We don't
        # want to have to worry about whether 'args' and 'kwargs' are
        # sortable, so all _BATCH messages have a serial number, and
        # therefore are guaranteed to be processed in the same order
        # as calls to queue_batch.
        self._batch_queue.put((self._BATCH, self._serializer, args, kwargs))
        self._serializer += 1

    def finished(self):
        self._batch_queue.put((self._DONE,))

    # subclass API
    def process_batch(self, *args, **kwargs):
        raise NotImplemented

    def is_interrupted(self):
        # There is no way to peek at the contents of a PriorityQueue, but
        # because messages are totally ordered, we can just call get() and
        # then put() without perturbing anything.
        try:
            x = self._batch_queue.get_nowait()
            rv = x[0] == self._INTERRUPT
            self._batch_queue.put(x)
            return rv
        except queue.Empty:
            return False

    # main loop
    def __call__(self, mon, thr):
        self._mon = mon
        self._mon.register_event_queue(self._batch_queue, (self._INTERRUPT,))

        try:
            while True:
                try:
                    self._mon.set_status_prefix(self._prefix)
                    self.mon.report_status("idle")
                    self.idle = True
                    msg = self.batch_queue.get()

                    if msg[0] == self._MON_INTERRUPT:
                        self.mon.maybe_pause_or_stop()

                    elif msg[0] == self._DONE:
                        self.mon.report_status("done")
                        return

                    elif msg[0] == self._BATCH:
                        _, _, args, kwargs = msg
                        try:
                            self.idle = False
                            result = self.process_batch(*args, **kwargs)
                            if result is not None:
                                self._disp.complete_batch(self, result)
                        except Exception:
                            self._disp.fail_batch(self, sys.exc_info())
                            raise

                    else:
                        self.mon.report_error("invalid batch queue message {!r}"
                                              .format(msg))

                except Exception:
                    self.mon.report_exception()

        finally:
            self.disp.drop_worker(self)
