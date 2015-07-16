#! /usr/bin/python

# Note: this program requires the Banyan data structure library.
# https://pypi.python.org/pypi/Banyan

import argparse
import ast
import banyan
import collections
import dircache
import errno
import os
import re
import subprocess
import sys
import tempfile

class Fingerprint(list):
    def __init__(self, url):
        self.url = url

_workerlog_first_re = re.compile(
    r"^[0-9]+\.[0-9]+ worker [0-9a-z]+ using port ([0-9]+)$")
_workerlog_line_re = re.compile(
    r"^([0-9]+\.[0-9]+) (start|stop) +\([0-9]+, (.+)\)$")
class WorkerLogs(object):
    def __init__(self):
        self.ports = collections.defaultdict(
            lambda: banyan.SortedDict(
                key_type=(float, float),
                updator=banyan.OverlappingIntervalsUpdator))

    def read_logfile(self, fname):
        with open(fname, "r") as f:
            # The first line is expected to be of the form
            #    TIMESTAMP worker LABEL using port PORT
            # and we only care about the port number.
            try:
                first = f.next()
            except StopIteration:
                return
            m = _workerlog_first_re.match(first)
            try:
                port = int(m.group(1))
            except (AttributeError, ValueError):
                sys.stderr.write("{}: failed to parse first line, skipping\n"
                                 .format(fname))
                return

            intervals = self.ports[port]
            starttime = None
            starturl = None
            stoptime = None
            stopurl = None
            for i, line in enumerate(f):
                m = _workerlog_line_re.match(line)
                try:
                    timestamp = float(m.group(1))
                    what = m.group(2)
                    url = ast.literal_eval(m.group(3))
                    if not isinstance(url, basestring):
                        raise ValueError("not a valid string literal")
                except (AttributeError, ValueError) as e:
                    sys.stderr.write("{}:{}: parse error: {}\n"
                                     .format(fname, i+2, e))
                    continue

                if what == "start":
                    starttime = timestamp
                    starturl = url
                elif what == "stop":
                    stoptime = timestamp
                    stopurl = url

                    if starturl != stopurl:
                        sys.stderr.write("{}:{}: URL mismatch: "
                                         "start {} stop {}\n"
                                         .format(fname, i+2,
                                                 starturl, stopurl))
                        continue
                    if starttime > stoptime:
                        sys.stderr.write("{}:{}: impossible interval: "
                                         "start {:.6f} stop {:.6f}\n"
                                         .format(fname, i+2,
                                                 starttime, stoptime))
                        continue

                    intervals[(starttime, stoptime)] = Fingerprint(url)

                else:
                    sys.stderr.write("{}:{}: impossible 'what': {}"
                                     .format(fname, i+2, what))
                    continue

    def dump(self, out):
        portl = self.ports.keys()
        portl.sort()
        for port in portl:
            seq = self.ports[port]
            # discard ports that never actually logged any URLs
            if not seq: continue
#            out.write("\n>>> {}\n".format(port))
            for (iv, fp) in seq.items():
                out.write("{:.6f} .. {:.6f}  {}\n"
                          .format(iv[0], iv[1], fp.url))

def read_worker_logs(dirname):
    logs = WorkerLogs()
    for fname in dircache.listdir(dirname):
        if fname.endswith(".urls"):
            logs.read_logfile(os.path.join(dirname, fname))
    return logs

def process_packet(packet, worker_logs):
    pass

def process_packet_captures(server_ip, capture_files, worker_logs):
    env = os.environ[:]
    env["SERVER_IP"] = server_ip

    extract_sub = "-Xlua_script:" + \
        os.path.join(os.path.dirname(__file__),
                     "fingerprint_extract_tshark_sub.lua")

    devnull = os.open(os.devnull, os.O_RDONLY)

    # You can only read one capture file at a time with tshark.  If you
    # specify multiple "-r" options, the last one silently wins.  Feh.
    for cap in capture_files:
        shark = None
        try:
            shark = subprocess.Popen(["tshark", "-q", extract_sub, "-r", cap],
                                     stdin=devnull, stdout=subprocess.PIPE)
            for packet in shark.stdout:
                process_packet(packet, worker_logs)
        except:
            if shark is not None:
                shark.terminate()
            raise
        finally:
            if shark is not None:
                shark.wait()


class DirType(object):
    """Argparse validator object for a directory.
       If 'create' is True, the directory is created if it does not already
       exist; if it does exist, it is checked for writability.
       If 'create' is False, the directory must already exist, and must only
       be readable."""
    def __init__(self, create=False):
        self.create = create

    @staticmethod
    def check_writable_directory(path):
        # The simplest and most reliable way to determine whether a
        # directory is writable is to create a file there.
        with tempfile.TemporaryFile(prefix="write_check.", dir=path):
            pass

    def __call__(self, path):
        try:
            if self.create:
                try:
                    os.mkdir(path)
                except EnvironmentError, e:
                    if e.errno != errno.EEXIST:
                        raise
                    self.check_writable_directory(path)
            else:
                # The simplest and most reliable way to determine whether
                # a directory is readable is to try to read it.  Python
                # provides no good way to open a directory for read without
                # actually reading it, so use dircache to stash its contents
                # for later use.  FIXME: not py3k-friendly :-/
                dircache.listdir(path)
            return path

        except EnvironmentError, e:
            if self.create: kind = "writable"
            else:           kind = "readable"
            raise argparse.ArgumentTypeError("%s: %s (need a %s directory)"
                                             % (path, e.strerror, kind))


def main():
    parser = argparse.ArgumentParser(description="Extract page fingerprints "
                                     "by correlating packet captures with "
                                     "URL logs.")
    parser.add_argument("server_ip",
                        help="IP address of proxy server")
    parser.add_argument("srcdir", type=DirType(),
                        help="directory containing packet captures "
                        "and URL logs")
    parser.add_argument("dstdir", type=DirType(create=True),
                        help="directory to write fingerprints to "
                        "(will be created if necessary)")

    args = parser.parse_args()

    logs = read_worker_logs(args.srcdir)
    logs.dump(sys.stdout)

if __name__ == '__main__': main()
