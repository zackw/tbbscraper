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

from selenium.webdriver import Firefox
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary

from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def patch_file(fname, workdir):
    (fd, tmpname) = tempfile.mkstemp(prefix="pat_",
                                     dir=os.path.dirname(fname),
                                     text=True)
    ouf = os.fdopen(fd, "w")
    inf = open(fname, "rU")
    for line in inf:
        ouf.write(line.replace("@WORKDIR@", workdir))
    inf.close()
    ouf.close()
    os.rename(tmpname, fname)

class TbbDriver(object):
    def __init__(self, bundle_dir="/usr/lib/tor-browser"):
        self.bundle_dir = bundle_dir
        self.work_dir = None
        self.driver = None
        self.server = None

    def __enter__(self):
        try:
            self.work_dir = tempfile.mkdtemp(prefix="tbb_", dir=os.getcwd())
            for src in glob.iglob(os.path.join(self.bundle_dir, "Data/*")):
                dst = os.path.join(self.work_dir, os.path.basename(src))
                if os.path.isdir(src):
                    shutil.copytree(src, dst)
                else:
                    shutil.copyfile(src, dst)

            os.mkdir(os.path.join(self.work_dir, "Tor", "data"))
            patch_file(os.path.join(self.work_dir, "Tor", "torrc"),
                       self.work_dir)
            patch_file(os.path.join(self.work_dir,
                                    "profile", "preferences",
                                    "extension-overrides.js"),
                       self.work_dir)

            #self.server = subprocess.Popen(["selenium-server"])

            os.environ["LD_LIBRARY_PATH"] = \
                os.path.join(self.bundle_dir, "App", "Firefox") + ":" + \
                os.path.join(self.bundle_dir, "Lib")

            profile = FirefoxProfile(os.path.join(
                    self.work_dir, "profile"))
            binary = FirefoxBinary(os.path.join(
                    self.bundle_dir, "App", "Firefox", "firefox"))
            self.driver = Firefox(firefox_profile = profile,
                                  firefox_binary  = binary)

            # We may have to wait *quite some time* for the browser + Tor
            # client to spool up.
            to_torcheck = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located(
                    By.xpath("//a[@href='https://check.torproject.org/']")))

            to_torcheck.click()
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    By.xpath("//text()[contains(., 'This page is also available in the following languages:')]")))

            if driver.find_element_by_xpath("//img[@src='/images/tor-on.png']"):
                # Success!
                return driver

            raise RuntimeError("Tor Browser Bundle did not come up correctly.")

        except:
            # Undo any partial construction that may have happened.
            self.__exit__()
            raise


    def __exit__(self, *dontcare):
        if self.driver is not None:
            self.driver.quit()
            self.driver = None
        if self.server is not None:
            self.server.terminate()
            self.server.wait()
            self.server = None
        if self.work_dir is not None:
            #shutil.rmtree(self.work_dir, ignore_errors=True)
            self.work_dir = None

if __name__ == '__main__':
    import time
    with TbbDriver() as driver:
        driver.get("https://duckduckgo.com/?q=muskrat+muskrat+muskrat")
        time.sleep(60)
