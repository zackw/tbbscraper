# Copyright © 2014-2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

# Wrapper library around OpenWPM for use with proxies.

import asyncio
import collections
import os
import queue
import random
import shutil
import signal
import subprocess
import threading

from concurrent import futures as cf
from urllib.parse import urlsplit

# we need a secure RNG in one place below, to generate unpredictable
# authentication tokens
rng = random.SystemRandom()

# Monkey-patch Selenium to start firejailed browsers.  Must do this
# before loading OpenWPM, so that OpenWPM sees the modification.
from selenium.webdriver.firefox import firefox_binary

class FirejailedFirefoxBinary(firefox_binary.FirefoxBinary):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._network_namespace = None

    def set_network_namespace(self, ns):
        self._network_namespace = ns

    def _start_from_profile_path(self, path):
        # Note: ignores self._start_cmd and invokes whatever "firefox"
        # happens to be in $PATH.  This is necessary in order to get the
        # right firejail profile without knowing where firejail keeps it.
        # (--profile=firefox ought to work, but it doesn't; you have to
        # supply an absolute path.)

        # Note 2: omitting the call to _modify_link_library_path
        # (thus, not loading x_ignore_nofocus.so) for now.
        # I _suspect_ this is papering over the absence of a window
        # manager, and I also suspect it could cause unpredictable,
        # unrelated problems.  firejail's headless mode does provide a
        # window manager so we shouldn't need it.

        self._firefox_env["XRE_PROFILE_PATH"] = path
        command = ["firejail", "--x11=xvfb"]
        if self._network_namespace is not None:
            command.append("--netns=" + self._network_namespace)
        command.extend(["--", "firefox", "-foreground"])

        if self.command_line:
            command.extend(self.command_line)
        self.process = subprocess.Popen(
            command, stdout=self._log_file, stderr=subprocess.STDOUT,
            env=self._firefox_env)

firefox_binary.OrigFirefoxBinary = firefox_binary.FirefoxBinary
firefox_binary.FirefoxBinary = FirejailedFirefoxBinary

from openwpm.automation import TaskManager
from openwpm.automation.DeployBrowsers.deploy_firefox import deploy_firefox
from openwpm.automation.MPLogger import loggingclient
from openwpm.automation.Commands import browser_commands as bcmd
from openwpm.automation.SocketInterface import clientsocket

class BrowserManager:
    """Global state associated with OpenWPM.
       We don't use the OpenWPM TaskManager, because we need to control
       starting and stopping of browser instances ourselves, and because
       we're bypassing the OpenWPM database.
    """
    def __init__(self, loop, data_directory,
                 manager_overrides={},
                 browser_overrides={}):
        manager_params, browser_params = TaskManager.load_default_params(1)

        manager_params["data_directory"] = data_directory
        manager_params["log_directory"] = data_directory
        manager_params.update(manager_overrides)

        browser_params = browser_params[0]
        browser_params["disable_flash"] = True
        browser_params["http_instrument"] = False
        browser_params["extension_enabled"] = False
        browser_params.update(browser_overrides)

        self.loop = loop
        self.manager_params = manager_params
        self.browser_params = browser_params
        self.logger = loggingclient(*manager_params["logger_address"])

        self.active_browsers = set()

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.loop.run_until_complete(
            asyncio.wait((b.close() for b in self.active_browsers),
                         loop=self.loop))

    def add_browser(self, browser):
        self.active_browsers.add(browser)

    def drop_browser(self, browser):
        self.active_browsers.discard(browser)

    @asyncio.coroutine
    def start_browser(self, proxy):
        """Start a new browser associated with proxy PROXY (an
           aioproxies.ProxyManager object).  Returns a Browser object.
        """
        b = Browser(self, proxy, loop)
        yield from b.start()
        return b

class BrowserWatchdog(threading.Thread):
    def __init__(self, *args, browser=None, **kwargs):
        threading.Thread.__init__(*args, **kwargs)
        self._browser = browser
        self._pq = queue.Queue()

    def run(self):
        while True:
            pid = self._pq.get()
            status = os.waitpid(pid)
            self._browser.manager.logger.debug(
                "browser %i: process %d exit %d"
                % (self._browser.tag, pid, status))
            if not self._browser.running:
                break
            self._browser.manager.logger.warning(
                "browser %i: crashed, restarting"
                % (self._browser.tag, pid))
            self._browser.restart()

        # break reference loop
        del self._browser

    def browser_started(self, pid):
        self._pq.put(pid)

# Subroutines of Browser.visit_url which do not need to be methods.
def extract_status_from_har(har, url):
    # URL _should_ be the final redirection destination, so we can
    # save some time in the normal case by just scanning for it.
    # Only if we're wrong do we go back and build up an index.
    resp = None
    for e in har["log"]["entries"]:
        if e["request"]["url"] == url:
            resp = e["response"]
            # Sometimes when you go to example.com/ with no cookies, you
            # get redirected to example.com/set-cookie and then back to
            # example.com. So the first instance of URL in HAR may be a
            # redirect even if URL really was the final destination.
            if resp.get("redirectURL", "") == "":
                break

    if resp is None or resp.get("redirectURL", "") != "":
        responses = collections.defaultdict(list)
        visits = collections.Counter()

        for e in har["log"]["entries"]:
            responses[e["request"]["url"]].append(e["response"])

        if url not in responses_by_url:
            url = har["log"]["entries"][0]["request"]["url"]

        while True:
            resp = responses[url][visits[url]]
            redir = resp.get("redirectURL", "")
            if not redir: break
            visits[url] += 1
            url = redir

    return { "status": resp["status"], "detail": resp.get("statusText", "") }

def stash_additional_status_in_har(har, addl_status):
    if not addl_status: return
    assert "_additional_status" not in har["log"]
    har["log"]["_additional_status"] = addl_status

@asyncio.coroutine
def get_neterr_details(url, nnsp, *, loop):
    surl = urlsplit(url)
    cmd = ["firejail", "--netns=" + nnsp, "--", "neterr-details"]
    if surl.scheme == "https":
        cmd.extend(["--tls", "--alpn=h2:http/1.1"])
        port = 443
    elif surl.scheme == "http":
        port = 80
    else:
        raise RuntimeError("get_neterr_details: don't know how to handle "
                           "scheme " + surl.scheme)
    if surl.port is not None:
        port = surl.port
    cmd.extend([surl.hostname, str(port)])

    proc = yield from asyncio.create_subprocess_exec(
        cmd, loop=loop,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    (stdout_data, stderr_data) = yield from proc.communicate()
    exitcode = yield from proc.wait()
    if exitcode != 0 or stderr_data != b"":
        raise RuntimeError("get_neterr_details: subprocess exit {}, "
                           "errors:\n{}"
                           .format(exitcode, stderr_data.decode("utf-8")))
    details = collections.defaultdict(list)
    for line in stdout_data.splitlines():
        k, _, v = line.partition(":")
        details[k].append(v)
    return details

class Browser:
    TAGGER = 0

    """One browser running under a particular proxy."""
    def __init__(self, manager, proxy, loop):
        self.tag = Browser.TAGGER
        Browser.TAGGER += 1

        self.manager   = manager
        self.proxy     = proxy
        self.loop      = loop
        self.watchdog  = BrowserWatchdog(browser=self)
        self.running   = False
        self.pid       = None
        self.profile   = None
        self.profile2  = None
        self.driver    = None
        self.crash_ev  = asyncio.Event(loop=self.loop)
        self.ready_ev  = asyncio.Event(loop=self.loop)
        self.visit_id  = 0
        self.har_token = "{:x}".format(rng.getrandbits(128))

        self.browser_params = manager.browser_params.copy()
        self.browser_params["network_namespace"] = self.proxy.get_namespace()
        self.browser_params["crawl_id"] = self.tag

    def __hash__(self):
        return self.tag
    def __eq__(self, other):
        if not isinstance(other, type(self)): return False
        return self.tag == other.tag
    def __ne__(self, other):
        return not (self == other)

    # These should properly be __aenter__ and __aexit__ but we still
    # need to work with Python 3.4.
    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.loop.run_until_complete(self.stop())

    @asyncio.coroutine
    def start(self):
        if self.running:
            raise RuntimeError("started twice")
        self.running = True
        self.watchdog.start()
        yield from self.internal_restart()
        self.manager.add_browser(self)

    def restart(self):
        self.internal_cleanup()
        return asyncio.run_coroutine_threadsafe(
            self.internal_restart, self.loop).result()

    @asyncio.coroutine
    def internal_restart(self):
        q = queue.Queue()
        yield from asyncio.wait([
            self.loop.run_in_executor(self.internal_start, q),
            self.loop.run_in_executor(self.internal_start_qworker, q)
        ], loop=self.loop)
        self.crash_ev.clear()
        self.ready_ev.set()

    @asyncio.coroutine
    def stop(self):
        if self.running:
            self.manager.forget_browser(self)
            self.running = False
            yield from self.loop.run_in_executor(
                self.internal_stop)

    @asyncio.coroutine
    def visit_url(self, url, timeout):
        """Load URL and report the results.
           Returns a 5-tuple (url, final_url, status, page_html, har).
        """
        if not self.running:
            raise RuntimeError("visit_url called when not running")

        yield from self.ready_ev.wait()

        visit_task = self.loop.run_in_executor(
                self.internal_visit_url, url)
        crash_task = self.crash_ev.wait()

        fin, pen = yield from asyncio.wait(
            [visit_task, crash_task],
            loop=self.loop, timeout=timeout,
            return_when=cf.FIRST_COMPLETED)

        for fut in pen:
            fut.cancel()
            try:
                yield from fut
            except asyncio.CancelledError:
                pass

        if len(fin) == 0:
            return (url, "timeout", "", "", {})

        if fin[0] is crash_task:
            fin[0].result()
            return (url, "browser crashed", "", "", {})

        (final_url, page_html, har) = fin[0].result()
        full_status = extract_status_from_har(har, final_url)
        if not status:
            full_status = yield from get_neterr_details(
                final_url, self.browser_params["network_namespace"],
                loop=self.loop
            )
        status = full_status.pop("status")
        detail = full_status.pop("detail", "")
        stash_additional_status_in_har(har, full_status)

        if status and detail:
            detail = status + " " + detail

        return (final_url, status, detail, page_html, har)

    def internal_start(self, q):
        driver, profile_path, settings = deploy_firefox(
            q, self.browser_params, self.manager.manager_params, False)
        self.driver = driver
        self.profile2 = profile_path


    def internal_start_qworker(self, q):
        while True:
            message = q.get()
            if (type(message) == tuple and len(message) == 3 and
                message[0] == "STATUS"):
                self.manager.logger.debug(
                    "browser %i(%s): %s: %r" % (
                        self.tag,
                        self.browser_params["network_namespace"],
                        message[1], message[2]))

                if message[1] == "Browser Launched":
                    self.pid = message[2][0]
                    self.watchdog.browser_started(pid)
                    break
                elif message[1] == "Profile Created":
                    self.profile = message[2]

            else:
                self.manager.logger.debug(
                    "browser %i(%s): odd startup queue message: %r" % (
                        self.tag,
                        self.browser_params["network_namespace"],
                        message[1]))

    def internal_stop(self):
        self.driver.quit()
        self.watchdog.join(timeout=5)

        if self.watchdog.is_alive():
            self.manager.logger.warning(
                "browser %i: driver.quit() failed, killing" % self.tag)
            os.kill(self.pid, signal.SIGTERM)
            self.watchdog.join(timeout=5)
            if self.watchdog.is_alive():
                self.manager.logger.warning(
                    "browser %i: SIGTERM failed, trying SIGKILL" % self.tag)
                os.kill(self.pid, signal.SIGKILL)
            self.watchdog.join(timeout=5)
            if self.watchdog.is_alive():
                self.manager.logger.warning(
                    "browser %i: SIGKILL failed, giving up" % self.tag)
                # Dirty: forcibly convert the watchdog into a daemon thread
                # so that it does not block process shutdown.
                if hasattr(self.watchdog, "_daemonic"):
                    self.watchdog._daemonic = True

        self.internal_cleanup()

    def internal_cleanup():
        def log_rmtree_error(fn, path, exc_info):
            self.manager.logger.warning(
                "%s: %s: %s" % (fn, path, exc_info[1].strerror))

        self.ready_ev.clear()
        self.crash_ev.set()
        self.pid = None
        self.driver = None

        if self.profile is not None:
            shutil.rmtree(self.profile, onerror=log_rmtree_error)
            self.profile = None

        if self.profile2 is not None:
            shutil.rmtree(self.profile, onerror=log_rmtree_error)
            self.profile2 = None

    def internal_visit_url(self, url):
        self.visit_id += 1
        bcmd.tab_restart_browser(self.driver)
        self.driver.execute_async_script("""
            var done = arguments[arguments.length-1];
            window['HAR_pageReady'] = false;
            window.addEventListener('har-page-ready', function (e) {
                window['HAR_pageReady'] = true;
            }, false);
            if (window.hasOwnProperty('HAR')) {
                done();
            } else {
                window.addEventListener('har-api-ready', function (e) {
                    done();
                }, false);
            }
        """)

        self.driver.get(url)
        har = self.driver.execute_async_script("""
            var done = arguments[arguments.length-1];
            function doExport() {
                window.HAR.triggerExport({
                    token: "%s", getData: true
                }).then(function (result) { done(result.data); })
            }
            if (window.HAR_pageReady) {
                doExport();
            } else {
                window.addEventListener('har-page-ready', doExport, false);
            }
        """ % self.har_token)

        final_url = self.driver.url
        page_html = self.driver.page_source.encode("utf8")

        bcmd.close_other_windows(self.driver)
        bcmd.bot_mitigation(self.driver)

        return (final_url, page_html, har)
