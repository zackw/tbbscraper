# Copyright © 2014-2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

# Wrapper library around OpenWPM for use with proxies.

# Monkey-patch Selenium to start firejailed browsers.  Must do this
# before loading OpenWPM, so that OpenWPM sees the modification.
import asyncio
import subprocess
from selenium.webdriver.firefox import firefox_binary

class FirejailedFirefoxBinary(firefox_binary.FirefoxBinary):
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
        command = [
            "firejail", "--x11=xvfb", "--",
            "firefox", "-foreground"
        ]
        if self.command_line:
            command.extend(self.command_line)
        self.process = subprocess.Popen(
            command, stdout=self._log_file, stderr=subprocess.STDOUT,
            env=self._firefox_env)

firefox_binary.OrigFirefoxBinary = firefox_binary.FirefoxBinary
firefox_binary.FirefoxBinary = FirejailedFirefoxBinary

from openwpm.automation import TaskManager, CommandSequence

class BrowserManager:
    """Global state associated with OpenWPM."""
    def __init__(self):
        raise NotImplemented

    def __enter__(self):
        pass

    def __exit__(self, *unused):
        pass

    @asyncio.coroutine
    def start_browser(proxy):
        """Start a new browser process associated with proxy PROXY
           (an aioproxies.ProxyManager object).  Returns a Browser
           object.
        """
        raise NotImplemented

class Browser:
    """One browser running under a particular proxy."""
    def __init__(self, manager, proxy, *etc):
        raise NotImplemented

    def __enter__(self):
        pass

    def __exit__(self, *unused):
        pass

    @asyncio.coroutine
    def visit_url(self, url):
        """Load URL and report the results.
           Returns a 4-tuple (status, final_url, html_content, capture_log).
        """
        raise NotImplemented
