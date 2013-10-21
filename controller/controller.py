#! /usr/bin/python

# This is the control program running on the eavesdropping entry node.
# As each worker bee checks in over zmq/ssh, it hands out entry ports and
# lists of URLs to retrieve.  It is responsible for supervising the Tor and
# tshark processes, and receiving updates from each worker with which to
# annotate the logged traffic.

import collections
import errno
import logging
import os
import cPickle as pickle
import pickletools
import random
import signal
import stat
import string
import subprocess
import sys
import threading
import time
import urlparse

import publicsuffix
import stem
import stem.control
import stem.process
import stem.util.log
import zmq

devnull = os.open(os.devnull, os.O_RDWR)

def pickled(cmd, *args):
    # The optimize() is here because pickle is tuned for backreferences at
    # the expense of wire output length when there are no backreferences.
    return pickletools.optimize(pickle.dumps((cmd, args),
                                             pickle.HIGHEST_PROTOCOL))

def unpickled(pickl):
    return pickle.loads(pickl)

def ensure_directory(path, mode=511): # 511 == 0777
    try:
        os.makedirs(path, mode)
        # Make sure the last path component has exactly the desired mode.
        os.chmod(path, mode)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise
        os.chmod(path, mode)

int2base_digs = string.digits + string.lowercase

def int2base(x, base):
  if x < 0: sign = -1
  elif x==0: return '0'
  else: sign = 1
  x *= sign
  digits = []
  while x:
    digits.append(int2base_digs[x % base])
    x /= base
  if sign < 0:
    digits.append('-')
  digits.reverse()
  return ''.join(digits)

def print_init_msg(line):
    sys.stderr.write("| " + line.strip() + "\n")

stem_log = None
def log_stem_trace(name):
    handler = logging.FileHandler(name)
    handler.setFormatter(logging.Formatter(
        fmt = '%(asctime)s [%(levelname)s] %(message)s',
        datefmt = '%m/%d/%Y %H:%M:%S',
    ))
    log = stem.util.log.get_logger()
    log.setLevel(stem.util.log.INFO)
    log.addHandler(handler)
    return log

class PipeDrainer(threading.Thread):
    """Read and discard all data from a pipe.  Exits when the pipe is closed."""
    def __init__(self, fp):
	threading.Thread.__init__(self)
        self.fp = fp 

    def run(self):
        try:
            while len(self.fp.read(8192)) > 0:
                pass
        except:
            pass
        self.fp.close()

class TorBridge(object):
    def __init__(self, cfg):
        self.cfg = cfg
        self.base_port = cfg.low_tor_port
        self.max_port  = cfg.high_tor_port
        self.ctl = None
        self.process = None
        self.datadir = None
        self.stem_log = None
        self.stdout_drainer = None
        self.stderr_drainer = None

    def __enter__(self):
        try:
            self.datadir = os.path.realpath(self.cfg.tor_data_dir)
            ensure_directory(self.datadir,
                             stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR)
            self.ctl_path = os.path.join(self.datadir, 'control')
            self.tor_log_path = os.path.join(self.datadir, 'tor.log')
            self.stem_log_path = os.path.join(self.datadir, 'stem.log')
            self.stem_log = log_stem_trace(self.stem_log_path)
            self.process = stem.process.launch_tor_with_config(config={
                    'Address'              : self.cfg.my_ip,
                    'Nickname'             : self.cfg.my_nickname,
                    'ContactInfo'          : self.cfg.my_contact,
                    'BandwidthRate'        : self.cfg.bandwidth,
                    'BandwidthBurst'       : self.cfg.bandwidth,

                    'BridgeRelay'          : '1',
                    'SocksPort'            : '0',
                    'ORPort'               : [str(self.base_port) +
                                              ' IPv4Only'],

                    'DataDirectory'        : self.datadir,

                    'ControlSocket'        : self.ctl_path,
                    'CookieAuthentication' : '1',
                    'Log'                  : ['notice stdout',
                                              'notice file '+self.tor_log_path],

                    'HardwareAccel'               : '1',
                    'ExitPolicy'                  : 'reject *:*',
                    'PublishServerDescriptor'     : '0',
                    'AssumeReachable'             : '1',
                    'ShutdownWaitLength'          : '10',
                    'BridgeRecordUsageByCountry'  : '0',
                    'DirReqStatistics'            : '0',
                    'ExtraInfoStatistics'         : '0',
                    'ExtendAllowPrivateAddresses' : '1'
                }, take_ownership=True, init_msg_handler=print_init_msg)

            self.stdout_drainer = PipeDrainer(self.process.stdout)
            self.stdout_drainer.start()
            self.stderr_drainer = PipeDrainer(self.process.stderr)
            self.stderr_drainer.start()

            self.ctl = stem.control.Controller.from_socket_file(self.ctl_path)
            self.ctl.authenticate()
            return self

        except:
            # undo any partial construction
            self.__exit__()
            raise

    def __exit__(self, *dontcare):
        self.stem_log.debug("TorBridge.__exit__")
        if self.ctl is not None:
            self.ctl.signal(stem.Signal.SHUTDOWN)
            self.ctl.close()
            self.process.wait()
        elif self.process is not None:
            self.process.send_signal(signal.SIGINT)
            self.process.wait()
        if self.stderr_drainer is not None:
            self.stderr_drainer.join()
        if self.stdout_drainer is not None:
            self.stdout_drainer.join()

    def add_client_port(self):
        # Note that the "base port" is never actually used by a client.
        # (However, the Tor process may use it internally.)
        self.stem_log.debug("TorBridge.add_client_port...")
        portlist = self.ctl.get_conf("ORPort", multiple=True)
        ports_in_use = frozenset(int(port.partition(' ')[0])
                                 for port in portlist)
        for port in xrange(self.base_port, self.max_port):
            if port not in ports_in_use:
                break
        else:
            return None
        self.stem_log.debug("TorBridge.add_client_port: selected port " + str(port))
        portlist.append(str(port) + " IPv4Only")
        self.ctl.set_conf("ORPort", portlist)
        self.stem_log.debug("TorBridge.add_client_port: ok")
        return port

    def del_client_port(self, port):
        sport = str(port)
        sports = sport + " "
        self.stem_log.debug("TorBridge.del_client_port: " + sport)
        portlist = [p for p in self.ctl.get_conf("ORPort", multiple=True)
                    if (p != sport and not p.startswith(sports))]
        self.ctl.set_conf("ORPort", portlist)
        self.stem_log.debug("TorBridge.del_client_port: ok")

class Client(object):

    _COUNT = 0

    def __init__(self):
        # This takes no arguments because it is called from
        # defaultdict.__missing__.  Only the attributes that may be
        # referred to before setup() is called are established
        # (this includes many attributes that may be referenced from
        # teardown() if setup() throws).
        self.was_setup    = False
        self.last_event   = time.time()
        self.bridge       = None
        self.shark        = None
        #self.pkt_log_fp   = None
        self.url_log_fp   = None
        self.queue        = None
        self.current_url  = None

        self.tag = int2base(Client._COUNT, 32)
        Client._COUNT += 1

    def setup(self, bridge, queue):
        self.queue = queue

        #self.pkt_log_name  = "worker-" + self.tag + ".pkts"
        self.url_log_name  = "worker-" + self.tag + ".urls"
        #self.pkt_log_fp    = open(self.pkt_log_name, "a")
        self.url_log_fp    = open(self.url_log_name, "a")

        self.bridge       = bridge
        self.port         = bridge.add_client_port()
        # self.shark        = subprocess.Popen(
        #     [#"dumpcap", "-w", self.pkt_log_name, "-f",
        #      "tshark", "-q", "-l", "-Xlua_script:fingerprint_extract.lua",
        #      "ip host {} and tcp port {}".format(bridge.cfg.my_ip,
        #                                          self.port)],
        #     stdin=devnull,
        #     stdout=self.pkt_log_fp,
        #     stderr=devnull,
        #     close_fds=True)
        #self.pkt_log_fp.close()
        #self.pkt_log_fp = None
        self.was_setup = True

    def teardown(self):
        # Note: must be idempotent.
        if self.bridge:
            self.bridge.del_client_port(self.port)
            self.bridge = None
            self.port = None

        if self.shark is not None:
            try:
                self.shark.terminate()
            except OSError, e:
                if e.errno != errno.ESRCH:
                    raise
            self.shark.wait()
            self.shark = None

        # if self.pkt_log_fp is not None:
        #     self.pkt_log_fp.close()
        #     self.pkt_log_fp = None
        if self.url_log_fp is not None:
            self.url_log_fp.close()
            self.url_log_fp = None

        if self.queue is not None:
            if self.current_url is not None:
                self.queue.append(self.current_url, putting_back=True)
                self.current_url = None
            self.queue = None

        return pickled("DONE")

    def protocol_error(self, msg, *inserts):
        full_msg = "protocol error: " + msg.format(*inserts)
        sys.stderr.write("\n" + tag + ": " + full_msg + "\n")
        self.url_log_fp.write("{:.6f} {}\n".format(self.last_event, full_msg))
        return self.teardown()

    def process(self, cmd, args):
        self.last_event = time.time()

        if cmd == "HELO" and len(args) == 0:
            return self.HELO()
        if cmd == "NEXT" and len(args) == 1:
            return self.NEXT(*args)
        if cmd == "URLS" and len(args) == 2:
            return self.URLS(*args)

        return self.protocol_error("unrecognized or invalid client request: "
                                   "{!r}{!r}", cmd, args)

    def HELO(self):
        if self.was_setup or self.bridge is not None:
            return self.protocol_error("HELO out of sequence")

        self.setup(bridge, cfg.urls)
        self.url_log_fp.write("{:.6f} worker {} using port {}\n"
                              .format(self.last_event, self.tag, self.port))
        return pickled("HELO",
                       self.bridge.cfg.my_ip,
                       str(self.port),
                       self.bridge.cfg.my_nickname,
                       self.bridge.cfg.my_family)

    def NEXT(self, current):
        if not self.was_setup:
            return self.protocol_error("NEXT before HELO")
        if self.current_url is not None:
            return self.protocol_error("expected URLS")
        if self.bridge is None:
            return self.protocol_error("NEXT after DONE")

        if not self.queue:
            # unlike all the above, this is not an error.
            # we've reached the end of the job.
            self.url_log_fp.write("{:.6f} worker {} done\n"
                                  .format(self.last_event, self.tag))
            return self.teardown()

        self.current_url = self.queue.nextafter(current)
        self.url_log_fp.write("{:.6f} start {}\n".format(self.last_event,
                                                         self.current_url))
        self.url_log_fp.flush()
        return pickled("LOAD", *self.current_url)

    def URLS(self, depth, urls):
        if not self.was_setup:
            return self.protocol_error("URLS before HELO")
        if self.current_url is None:
            return self.protocol_error("URLS without NEXT")
        if self.bridge is None:
            return self.protocol_error("URLS after DONE")

        self.url_log_fp.write("{:.6f} stop  {}\n".format(self.last_event,
                                                         self.current_url))
        self.url_log_fp.flush()
        self.current_url = None

        self.queue.extend((depth, url) for url in urls)
        return pickled("OK")

def gen_n_at_depth(maxdepth):
    tfib = [1,2,3,5,8,13,21,34,55,89,144]
    rv = tfib[:(maxdepth+1)]
    rv.reverse()
    return rv

class SitePartitionedURLQueue(object):
    def __init__(self, maxdepth, initial):
        self.site_extractor = publicsuffix.PublicSuffixList()
        self.queues   = collections.defaultdict(collections.deque)
        self.seen_url = set()
        self.maxdepth = maxdepth
        self.n_at_depth = gen_n_at_depth(maxdepth)
        self.extend(initial)

    def __nonzero__(self):
        return any(self.queues.itervalues())

    def internal_append(self, url, depth, putting_back):
        self.seen_url.add(url)
        site = self.site_extractor.get_public_suffix(
            urlparse.urlparse(url).hostname)
        q = self.queues[site]
        if putting_back:
            q.appendleft((depth, url))
        else:
            q.append((depth, url))

    def append(self, x, putting_back=False):
        if isinstance(x, tuple):
            depth, url = x
        else:
            depth = 0
            url = x
        if isinstance(url, unicode):
            url = url.encode('utf-8')
        if not putting_back:
            if url in self.seen_url: return
            if depth > maxdepth: return

    def extend(self, iterable, putting_back=False):
        if putting_back:
            for x in iterable: self.append(x, True)
        else:
            # Extending is stochastic.  We take all depth-0
            # urls (that haven't already been seen),
            # self.n_at_depth[depth-1] urls per site for
            # 1 <= depth <= maxdepth, and none deeper.
            bydepth = [ [] for _ in xrange(0, self.maxdepth+1) ]
            for x in iterable:
                if isinstance(x, tuple):
                    depth, url = x
                else:
                    depth = 0
                    url = x
                if isinstance(url, unicode):
                    url = url.encode('utf-8')
                if depth <= self.maxdepth and url not in self.seen_url:
                    bydepth[depth].append(url)

            for url in bydepth[0]:
                self.internal_append(url, 0, False)

            for depth in xrange(1, self.maxdepth+1):
                n = self.n_at_depth[depth-1]
                for url in random.sample(bydepth[depth],
                                         min(n, len(bydepth[depth]))):
                    self.internal_append(url, depth, False)

    def nextafter(self, url):
        # Prefer a page load from the current site if possible;
        # otherwise pick a nonempty site at random.
        if url is not None:
            site = self.site_extractor.get_public_suffix(
                urlparse.urlparse(url).hostname)
            if self.queues[site]:
                return self.queues[site].pop()

        for q in self.queues.itervalues():
            if q:
                return q.pop()

        return None

def controller_loop(cfg, bridge):
    ctx  = zmq.Context()
    sock = ctx.socket(zmq.ROUTER)
    sock.setsockopt(zmq.LINGER, 0)
    sock.bind(cfg.controller_address)

    clients = collections.defaultdict(Client)

    try:
        while cfg.urls or any(c.bridge for c in clients.values()):
            if sock.poll(timeout = 60 * 1000):
                (address, _, query) = sock.recv_multipart()
                (cmd, args) = unpickled(query)
                client = clients[address]
                sys.stderr.write(" ")
                sys.stderr.write(client.tag)
                reply = client.process(cmd, args)
                sock.send_multipart([address, "", reply])
            else:
                sys.stderr.write(" $")

            # Check for clients that haven't made forward progress in a while.
            # We define "a while" as 10 minutes.
            now = time.time()
            for c in clients.values():
                if now - c.last_event > 10*60:
                    c.teardown()

            # Check whether we have lost all our clients.
            if cfg.urls and clients and \
                    not any(c.bridge for c in clients.values()):
                if all(now - c.last_event > 20*60 for c in clients.values()):
                    sys.stderr.write("\nno live clients in 20 minutes, exiting")
                    break
    finally:
        for c in clients.values():
            c.teardown()
        ctx.destroy()
        sys.stderr.write("\n")

class Config(object):
    """Global configuration."""
    def __init__(self, config_file):
        with open(config_file, "rU") as f:
            for line in f:
                if line == "": continue
                if line[0] in " \t":
                    line = line.strip()
                    if line == "": continue
                    setattr(self, k, getattr(self, k) + line)
                else:
                    line = line.strip()
                    k, _, v = line.partition("=")
                    k = k.rstrip()
                    v = v.lstrip()
                    setattr(self, k, v)

        self.low_tor_port = int(self.low_tor_port)
        self.high_tor_port = int(self.high_tor_port)
        self.maxdepth = int(self.maxdepth)

        urls = [(0, l.strip()) for l in open(self.url_list, "rU")]
        if hasattr(self, 'url_sample_size'):
            urls = random.sample(urls, int(self.url_sample_size))
        self.urls = SitePartitionedURLQueue(self.maxdepth, urls)

if __name__ == '__main__':
    cfg = Config(sys.argv[1])
    with TorBridge(cfg) as bridge:
        controller_loop(cfg, bridge)
