#! /usr/bin/python

# This is the control program running on the eavesdropping entry node.
# It starts all the worker bees over ssh tunnels.  It is responsible
# for handing out entry-node ports and lists of URLs to retrieve,
# supervising the Tor and tshark processes, and receiving updates from
# each worker bee with which to annotate the logged traffic.

import execnet
import os
import signal
import subprocess
import sys
import tempfile
import time

import tbbselenium

devnull = os.open(os.devnull, os.O_RDWR)

class Client(object):
    """A Client represents one remote worker.  It has a dedicated ORPort on
       this side, with a dedicated tshark process snooping on it.  (There is
       only one Tor process for all the clients.)"""

    def __init__(self, client_ip, this_ip, orport):
        self.active        = False
        self.client_ip     = client_ip
        self.this_ip       = this_ip
        self.orport        = orport
        self.nurls         = 0
        log_basename = client_ip.replace(".", "-")
        self.pkt_log_name  = log_basename + ".pkts"
        self.url_log_name  = log_basename + ".urls"
        self.pkt_log_fp    = open(self.pkt_log_name, "a")
        self.url_log_fp    = open(self.url_log_name, "a")

        self.shark_process = subprocess.Popen(
            ["tshark", "-q", "-Xlua_script:fingerprint_extract.lua",
             "ip host {} and tcp port {}".format(this_ip, orport)],
            stdin=devnull,
            stdout=self.pkt_log_fp,
            stderr=devnull,
            close_fds=True)
        self.pkt_log_fp.close()

        self.worker = execnet.makegateway()
        self.worker_chan = self.worker.remote_exec(tbbselenium)
        self.url_chan = self.worker.newchannel()
        self.url_chan.setcallback(self.log_url, None)
        self.worker_chan.send((self.this_ip, self.orport, self.url_chan))

    def log_url(self, url):
        if url is None:
            self.shark_process.terminate()
            self.shark_process.wait()
            self.url_log_fp.close()
        else:
            self.url_log_fp.write("{:.6f}|{}\n".format(time.time(), url))

def TorRelay(object):
    """Object responsible for starting up, configuring, and tearing down the
       Tor relay.  Use with 'with'."""
    def __init__(self, config):
        self.tor_binary = tor_binary
        self.tor_rc_path = "tbbscraper_tor.rc"
        self.tor_log_path = "tbbscraper_tor.log"

        tor_rc_fp = open(self.tor_rc_path, "w")
        tor_rc_fp.write("""\
BandwidthRate  {cf.bandwidth}
BandwidthBurst {cf.bandwidth}
DataDirectory  tbbscraper_tor.data
Log            notice stdout
HardwareAccel  1
Address        {cf.my_ip}
ExitPolicy     reject *:*
MyFamily       {cf.my_family}
Nickname       {cf.my_nickname}
PublishServerDescriptor 0
ShutdownWaitLength 10
""".format(cf=config))
        for port in sorted(config.clients.values()):
            self.tor_rc_fp.write("ORPort {ip}:{port} NoAdvertise IPv4Only\n"
                                 .format(ip=config.my_ip, port=port))
        self.tor_rc_fp.close()

    def __enter__(self):
        tor_log_fp = open(self.tor_log_path, "a")
        self.proc = subprocess.Popen([self.tor_binary, "-f", self.tor_rc_path],
                                     stdin=devnull,
                                     stdout=self.tor_log_fp,
                                     stderr=self.tor_log_fp,
                                     close_fds=True)
        tor_log_fp.close()

    def __exit__(self):
        self.proc.send_signal(signal.SIGINT)
        self.proc.wait()

def Config(object):
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

        client_ips = self.client_ips.split()
        self.low_tor_port = int(self.low_tor_port)
        self.high_tor_port = self.low_tor_port + len(client_ips) - 1
        self.client_ips = { ip : port
                            for ip, port in zip(client_ips,
                                                range(self.low_tor_port,
                                                      self.high_tor_port + 1)) }

#client = Client("127.0.0.1", "127.0.0.1", "9999")
#client.worker_chan.send([
#        "http://facebook.com/",
#        "http://google.com/",
#        "http://youtube.com/",
#        "http://yahoo.com/",
#        "http://baidu.com/",
#        "http://amazon.com/",
#        "http://qq.com/",
#        "http://live.com/",
#        "http://taobao.com/",
#        "http://wikipedia.org/",
#        "http://freefall.purrsia.com/"
#])
#client.worker_chan.send([])
#client.worker_chan.waitclose()
