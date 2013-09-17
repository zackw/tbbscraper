#! /usr/bin/python

# This is the program that will run on each worker-bee host.  It
# reaches out to the controller / entry node for its configuration,
# then spins up a TBB process under Selenium control, and then
# proceeds to load URLs as directed by the controller.  Each time a
# load completes, the controller is notified of the complete set of
# links (A tags) out from the page.  Deciding what to do with that is
# entirely up to the controller.

import glob
import os
import cPickle as pickle
import pickletools
import shutil
import subprocess
import sys
import tempfile
import time
import urlparse

import zmq
import zmq.ssh

from selenium.webdriver import Firefox
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary

from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium.common.exceptions import WebDriverException

def is_http(url):
    us = urlparse.urlsplit(url)
    return us.scheme == "http" or us.scheme == "https" or us.scheme == ""

def pickled(cmd, *args):
    # The optimize() is here because pickle is tuned for backreferences at
    # the expense of wire output length when there are no backreferences.
    return pickletools.optimize(pickle.dumps((cmd, args),
                                             pickle.HIGHEST_PROTOCOL))

def unpickled(pickl):
    return pickle.loads(pickl)

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
    """This class is geared to be used in a with-statement.
       with TbbDriver(...) as driver:
           # If control enters this block, a Tor Browser Bundle instance
           # was successfully started and navigated to
           # https://check.torproject.org/, and that page indicated that
           # we are in fact using Tor.  'driver' is the Selenium driver
           # object.
    """
    def __init__(self, entry_ip, entry_port, entry_node, exclude_nodes,
                 bundle_dir="/usr/lib/tor-browser"):
        self.entry_ip = entry_ip
        self.entry_port = entry_port
        self.entry_node = entry_node
        self.exclude_nodes = exclude_nodes
        self.bundle_dir = bundle_dir
        self.work_dir = None
        self.driver = None
        if not os.path.isdir(os.path.join(bundle_dir, "Data")):
            raise RuntimeError("no TBB found at " + bundle_dir)

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
ExcludeNodes {cf.exclude_nodes}
Bridge {cf.entry_ip}:{cf.entry_port}
UseBridges 1
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


class WorkerConnection(object):
    """This class is also geared to be used in a with-statement.
       with WorkerConnection(...) as conn:
           # if control reaches this point, we have a successful
           # connection to the controller host, and 'conn' will
           # reveal details about how Tor is to be started, then
           # provide the message queue.
    """
    def __init__(self, address, tunnel=None):
        self.tunnel = tunnel
        self.address = address
        self.req = None
        self.context = None
        self.done = False

    def __enter__(self):
        try:
            self.context = zmq.Context()
            self.req = self.context.socket(zmq.REQ)
            self.req.setsockopt(zmq.LINGER, 0)
            if self.tunnel:
                zmq.ssh.tunnel_connection(self.req, self.address, self.tunnel)
            else:
                self.req.connect(self.address)

            self.req.send(pickled("HELO"))
            (cmd, args) = unpickled(self.req.recv())
            if cmd != "HELO":
                if cmd == "DONE" and len(args) == 0:
                    self.done = True
                    return self

                raise RuntimeError("protocol error: expected HELO, got %s%s"
                                   % (repr(cmd), repr(args)))
            if len(args) != 4:
                raise RuntimeError("protocol error: expected 4 args to HELO"
                                   " (got %s)" % repr(args))
            (self.entry_ip, self.entry_port, self.entry_node,
             self.entry_family) = args
            return self

        except:
            # Undo any partial construction that may have happened.
            self.__exit__()
            raise

    def __exit__(self, *dontcare):
        if self.req is not None:
            self.req.close()
        if self.context is not None:
            self.context.destroy()

    def __iter__(self):
        return self

    def next(self):
        while True:
            if self.done:
                raise StopIteration

            self.req.send(pickled("NEXT"))
            (cmd, args) = unpickled(self.req.recv())
            if cmd == "DONE":
                if len(args) > 0:
                    raise RuntimeError("protocol error: DONE takes no args"
                                       " (got {!r})".format(args))
                self.done = True
                raise StopIteration
            elif cmd == "LOAD":
                if len(args) != 2:
                    raise RuntimeError("protocol error: LOAD takes two args"
                                       " (got {!r})".format(args))
                return args
            elif cmd == "WAIT":
                if len(args) != 1:
                    raise RuntimeError("protocol error: WAIT takes one arg"
                                       " (got {!r})".format(args))
                time.sleep(args[0])
                continue
            else:
                raise RuntimeError("protocol error: unrecognized command {}{!r}"
                                   .format(cmd, args))

    def report_urls(self, depth, urls):
        self.req.send(pickled("URLS", depth+1, urls))
        (cmd, args) = unpickled(self.req.recv())
        if cmd == "OK" and len(args) == 0:
            return
        if cmd == "DONE" and len(args) == 0:
            self.done = True
            return
        raise RuntimeError("protocol error: expected OK or DONE, got %s%s"
                           % (repr(cmd), repr(args)))

if __name__ == '__main__':
    def worker_bee(argv):
        with WorkerConnection(*argv) as conn:
            if conn.done: return
            with TbbDriver(conn.entry_ip,
                           conn.entry_port,
                           conn.entry_node,
                           conn.entry_family) as driver:
                link_elts = {}
                driver.implicitly_wait(60)
                driver.set_page_load_timeout(60)
                for (depth, url) in conn:
                    try:
                        # If this is a URL corresponding to a link on
                        # the current page, first try to load it by
                        # clicking the link.
                        #XXX temporarily disabled till we figure out how to
                        # deal with sketchy sites that open windows on click.
                        if False: #url in link_elts:
                            try:
                                link_elts[url].click()
                            except WebDriverException:
                                driver.get(url)
                        else:
                            driver.get(url)

                        link_elts.clear()
                        for elt in driver.find_elements_by_xpath("//a[@href]"):
                            try:
                                href = elt.get_attribute("href")
                                # We don't want to load non-HTTP URLs,
                                # links back to the current page, or
                                # links differing from the current
                                # page only in a fragment identifier.
                                # The controller will handle the
                                # general case of not repeating page
                                # loads within a site.
                                canon = urlparse.urldefrag(
                                    urlparse.urljoin(url, href))[0]
                                if canon != url and is_http(canon):
                                    link_elts[canon] = elt
                            except WebDriverException:
                                pass

                        sys.stderr.write("{}: depth {}, {} outbound links\n"
                                         .format(url, depth, len(link_elts)))
                        conn.report_urls(depth, link_elts.keys())

                    except WebDriverException, e:
                        sys.stderr.write("{}: depth {}, "
                                         "link extraction failure ({})\n"
                                         .format(url, depth, e.msg))
                        conn.report_urls(depth, [])

                    time.sleep(0.1)

    worker_bee(sys.argv[1:])
    sys.stdout.write("\nDONE\n")
