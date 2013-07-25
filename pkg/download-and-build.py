#! /usr/bin/python

import errno
import hashlib
import os
import subprocess
import sys

class PackageConstructor(object):
    def __init__(self, source_url, download_name, download_size, download_sha,
                 package_name, package_builder):
        self.source_url      = source_url
        self.download_name   = download_name
        self.download_size   = download_size
        self.download_sha    = download_sha.decode("hex")
        self.package_name    = package_name
        self.package_builder = package_builder

        self.download_mtime  = None
        self.package_mtime   = None

    def check_file(self):
        # Returns a 3-tuple:
        #   ( <Boolean: True if file already exists>
        #     <Boolean: True if file has correct size and checksum>
        #     <Integer: Present size of file> )

        good = False
        fd = None
        try:
            fd = os.open(self.download_name, os.O_RDONLY)
            st = os.fstat(fd)
            if st.st_size == self.download_size:
                hasher = hashlib.sha256()
                while True:
                    chunk = os.read(fd, 128 * 1024)
                    if len(chunk) == 0: break
                    hasher.update(chunk)
                good = hasher.digest() == self.download_sha

            if good:
                self.download_mtime = st.st_mtime

            return (True, good, st.st_size)

        except EnvironmentError, e:
            if e.errno == errno.ENOENT:
                return (False, False, 0)
            raise

        finally:
            if fd is not None: os.close(fd)

    def download_upstream(self):
        # If the file already exists, we might not need to download it.
        (exists, good, present_size) = self.check_file()
        if good:
            sys.stderr.write("%s: already downloaded\n" % self.download_name)
            return

        # If the present_size is greater than or equal to the desired size,
        # resuming the download will not work; we need to start over.
        if exists and present_size >= self.download_size:
            os.unlink(self.download_name)

        # It is substantially easier to make curl do a resume-if-necessary
        # file transfer than to do it ourselves with urllib(2).
        sys.stderr.write("downloading %s...\n" % self.download_name)
        subprocess.check_call(["curl", "-o", self.download_name, "-C", "-",
                               self.url])

        (exists, good, present_size) = self.check_file()
        if not good:
            if present_size != st.st_size:
                sys.stderr.write(
                    "%s: download failure: wanted %d bytes, got %d\n"
                    % (self.download_name, self.download_size, present_size))
            else:
                sys.stderr.write(
                    "%s: download failure: checksum mismatch\n"
                    % (self.download_name))
            raise RuntimeError

    def check_package(self):
        try:
            self.package_mtime = os.stat(self.package_name).st_mtime
            return self.package_mtime >= self.download_mtime
        except EnvironmentError, e:
            if e.errno == errno.ENOENT:
                return False
            raise

    def build_package(self):
        if self.check_package():
            sys.stderr.write("%s: package is up to date\n"
                             % self.package_name)
            return
        sys.stderr.write("building %s...\n" % self.package_name)
        self.package_builder()
        if not self.check_package():
            sys.stderr.write("%s: package build failed\n")
            raise RuntimeError

def BuildTBB():
    PKGDIR="tor-browser-3.0a2"

    # Clean the package directory before extracting anything.
    subprocess.check_call(["git", "clean", "-qdxf"], cwd=PKGDIR)

    # The upstream tarball has the wrong top-level directory name.
    # Fortunately, GNU tar lets us correct this.
    subprocess.check_call(["tar", "xa", "--strip=1",
                           "-f", "../tor-browser-3.0a2.tar.xz"],
                          cwd=PKGDIR)

    # All adjustments are handled via "format 3.0 (quilt)" patches.
    subprocess.check_call(["dpkg-buildpackage", "-b", "-uc"], cwd=PKGDIR)
    os.unlink("tor-browser_3.0~a2-1_amd64.changes")

def BuildPythonSelenium():
    PKGDIR="python-selenium-2.33.0"

    # Clean the package directory before extracting anything.
    subprocess.check_call(["git", "clean", "-qdxf"], cwd=PKGDIR)

    # The upstream tarball has the wrong top-level directory name.
    # Fortunately, GNU tar lets us correct this.
    subprocess.check_call(["tar", "xa", "--strip=1",
                           "-f", "../python-selenium-2.33.0.tar.gz"],
                          cwd=PKGDIR)

    # Remove an unwanted binary file.  (This is impractical from a
    # quilt patch.)
    os.unlink(PKGDIR+"/py/selenium/webdriver/firefox/x86/x_ignore_nofocus.so")
    os.rmdir (PKGDIR+"/py/selenium/webdriver/firefox/x86/")

    # All other adjustments are handled via "format 3.0 (quilt)" patches.
    subprocess.check_call(["dpkg-buildpackage", "-b", "-uc"], cwd=PKGDIR)
    os.unlink("python-selenium_2.33.0-1_amd64.changes")

def BuildSeleniumServer():
    # The download phase dropped the .jar inside the package directory,
    # so very little else needs doing.
    subprocess.check_call(["dpkg-buildpackage", "-b", "-uc"],
                          cwd="selenium-server-2.33.0")
    os.unlink("selenium-server_2.33.0-1_amd64.changes")

packages = [PackageConstructor(**spec) for spec in [
    { "source_url" :
          "https://archive.torproject.org/tor-package-archive/torbrowser/3.0a2/"
              "tor-browser-linux64-3.0-alpha-2_en-US.tar.xz",
      "download_name" : "tor-browser-3.0a2.tar.xz",
      "download_size" : 22835272,
      "download_sha" :
          "922f9662f029b99739cd2c7a8ceabf156305a93f748278f9d23b9471c5b1b619",
      "package_name" : "tor-browser_3.0~a2-1_amd64.deb",
      "package_builder" : BuildTBB
    },

    { "source_url" :
          "https://pypi.python.org/packages/source/s/selenium/"
              "selenium-2.33.0.tar.gz",
      "download_name" : "python-selenium-2.33.0.tar.gz",
      "download_size" : 2536129,
      "download_sha" :
          "6508690bad70881eb851c3921b7cb51faa0e3409e605b437058e600677ede89b",
      "package_name" : "python-selenium_2.33.0-1_amd64.deb",
      "package_builder" : BuildPythonSelenium
    },

    { "source_url" :
          "http://selenium.googlecode.com/files/"
              "selenium-server-standalone-2.33.0.jar",
      "download_name" : "selenium-server-2.33.0/selenium-server.jar",
      "download_size" : 34297072,
      "download_sha" :
          "68ba647e91d144d5b1bb2e0479774ebca5d4fc201566760735280c46e70a951e",
      "package_name" : "selenium-server_2.33.0-1_all.deb",
      "package_builder" : BuildSeleniumServer
    },
]]

for pkg in packages:
    pkg.download_upstream()

for pkg in packages:
    pkg.build_package()
