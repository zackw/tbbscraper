#! /usr/bin/python

# This does much the same stuff that the stock start-tor-browser shell
# script does, but also spins up a Selenium controller for the
# browser.  Intended usage is
#
#  with TbbDriver() as driver:
#      # If control enters this block, a Tor Browser Bundle instance
#      # was successfully started and navigated to
#      # https://check.torproject.org/, and that page indicated that
#      # we are in fact using Tor; 'driver' is the Selenium driver
#      # object.
#
# The elaborate "get an error message to the user by any means necessary"
# mechanism has been removed, since this is for scripting use.

import glob
import os
import shutil
import subprocess
import sys
import tempfile
import time

from selenium.webdriver import Firefox
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary

from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def patch_file(fname, workdir, append_text=""):
    (fd, tmpname) = tempfile.mkstemp(prefix="pat_",
                                     dir=os.path.dirname(fname),
                                     text=True)
    ouf = os.fdopen(fd, "w")
    inf = open(fname, "rU")
    for line in inf:
        ouf.write(line.replace("@WORKDIR@", workdir))
    if append_text:
        ouf.write(append_text)
    inf.close()
    ouf.close()
    os.rename(tmpname, fname)

class TbbDriver(object):
    def __init__(self, entry_ip, entry_port, entry_node, exclude_nodes,
                 bundle_dir="/usr/lib/tor-browser"):
        self.entry_ip = entry_ip
        self.entry_port = entry_port
        self.entry_node = entry_node
        self.exclude_nodes = exclude_nodes
        self.bundle_dir = bundle_dir
        self.work_dir = None
        self.driver = None

    def __enter__(self):
        try:
            self.work_dir = tempfile.mkdtemp(prefix="tbb_", dir=os.getcwd())
            for src in glob.iglob(os.path.join(self.bundle_dir, "Data/*")):
                dst = os.path.join(self.work_dir, os.path.basename(src))
                if os.path.isdir(src):
                    shutil.copytree(src, dst)
                else:
                    shutil.copyfile(src, dst)

            # The Tor/data directory needs to be mode 0700, and everything
            # in it needs to be mode 0600, or else Tor won't trust it.
            datadir = os.path.join(self.work_dir, "Tor", "data")
            os.chmod(datadir, 0700)
            for f in glob.iglob(os.path.join(datadir, "*")):
                os.chmod(f, 0600)

            patch_file(os.path.join(self.work_dir, "Tor", "torrc"),
                       self.work_dir, """\
#ExcludeNodes {cf.exclude_nodes}
Bridge {cf.entry_ip}:{cf.entry_port}
UseBridges 1
UseMicroDescriptors 0
Log debug stderr
""".format(cf=self))
            patch_file(os.path.join(self.work_dir,
                                    "profile", "preferences",
                                    "extension-overrides.js"),
                       self.work_dir)

            os.environ["LD_LIBRARY_PATH"] = \
                os.path.join(self.bundle_dir, "App", "Firefox") + ":" + \
                os.path.join(self.bundle_dir, "Lib")

            if "DISPLAY" not in os.environ:
                os.environ["DISPLAY"] = ":0"
                if "XAUTHORITY" not in os.environ:
                    os.environ["XAUTHORITY"] = os.path.join(os.environ["HOME"],
                                                            ".Xauthority")
                xerrors = os.open(os.path.join(os.environ["HOME"],
                                               ".xsession-errors"),
                                  os.O_WRONLY|os.O_CREAT|os.O_APPEND, 0666)
                os.dup2(xerrors, 1)
                os.dup2(xerrors, 2)
                os.close(xerrors)

            profile = FirefoxProfile(os.path.join(
                    self.work_dir, "profile"))
            binary = FirefoxBinary(os.path.join(
                    self.bundle_dir, "App", "Firefox", "firefox"))
            self.driver = Firefox(firefox_profile = profile,
                                  firefox_binary  = binary)

            # Control only reaches this point when the browser is
            # fully spooled up.  Make sure we are actually using Tor.
            # FIXME: I can't figure out how to do "wait until onload
            # fires, then see which image we have".

            self.driver.get("https://check.torproject.org/")
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH,
                     "//img[@src='/images/tor-on.png']")))

            return self.driver

        except:
            # Undo any partial construction that may have happened.
            self.__exit__()
            raise


    def __exit__(self, *dontcare):
        if self.driver is not None:
            self.driver.quit()
            self.driver = None
        if self.work_dir is not None:
            shutil.rmtree(self.work_dir, ignore_errors=True)
            self.work_dir = None

if __name__ == '__main__':
    import time
    with TbbDriver() as driver:
        driver.get("https://duckduckgo.com/?q=muskrat+muskrat+muskrat")
        time.sleep(10)

# When invoked by controller.py over execnet, __name__ is set this way.
elif __name__ == '__channelexec__':
    def readystate_complete(d):
        return d.execute_script("return document.readyState") == "complete"

    def worker_bee(channel):
        # The first thing sent over the master channel is information
        # about how we should run Tor, plus a subsidiary channel for
        # reporting back timestamped URLs.
        (entry_ip, entry_port, entry_node, entry_family, url_channel) = \
            channel.receive()
        with TbbDriver(entry_ip, entry_port, entry_node, entry_family) \
                as driver:
            wait = WebDriverWait(driver, 10)
            for block in channel:
                if len(block) == 0: break
                for url in block:
                    # Python is probably using gettimeofday(), so round to
                    # microseconds
                    url_channel.send("{:.6f}|{}".format(time.time(),url))
                    time.sleep(0.1)
                    driver.get(url)
                    wait.until(readystate_complete)
                    time.sleep(0.1)
        url_channel.close()

    worker_bee(channel)
