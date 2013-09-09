#! /usr/bin/python

# This is the control program running on the eavesdropping entry node.
# It starts all the worker bees over ssh tunnels.  It is responsible
# for handing out entry-node ports and lists of URLs to retrieve,
# supervising the Tor and tshark processes, and receiving updates from
# each worker bee with which to annotate the logged traffic.

import execnet
import itertools
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

    def __init__(self, client_ip, relay_ip,
                 relay_port, relay_node, relay_family):
        self.active        = False
        self.client_ip     = client_ip
        self.relay_ip      = relay_ip
        self.relay_port    = relay_port
        self.relay_node    = relay_node
        self.relay_family  = relay_family
        self.nurls         = 0

        log_basename = client_ip.replace(".", "-")
        self.pkt_log_name  = log_basename + ".pkts"
        self.url_log_name  = log_basename + ".urls"
        self.pkt_log_fp    = open(self.pkt_log_name, "a")
        self.url_log_fp    = open(self.url_log_name, "a")

        self.shark_process = subprocess.Popen(
            ["tshark", "-q", "-Xlua_script:fingerprint_extract.lua",
             "ip host {} and tcp port {}".format(self.relay_ip,
                                                 self.relay_port)],
            stdin=devnull,
            stdout=self.pkt_log_fp,
            stderr=devnull,
            close_fds=True)
        self.pkt_log_fp.close()

        self.worker = execnet.makegateway("ssh={}//"
                                          "ssh_config=controller_sshconfig"
                                          .format(client_ip))
        self.worker_chan = self.worker.remote_exec(tbbselenium)
        self.url_chan = self.worker.newchannel()
        self.url_chan.setcallback(self.log_url, None)
        self.worker_chan.send((self.relay_ip, self.relay_port, self.relay_node,
                               self.relay_family, self.url_chan))

    def log_url(self, url):
        if url is None:
            self.shark_process.terminate()
            self.shark_process.wait()
            self.url_log_fp.close()
        else:
            self.url_log_fp.write("{:.6f}|{}\n".format(time.time(), url))

class TorRelay(object):
    """Object responsible for starting up, configuring, and tearing down the
       Tor relay.  Use with 'with'."""
    def __init__(self, config):
        self.tor_binary = config.tor_binary
        self.tor_rc_path = "tbbscraper_tor.rc"
        self.tor_log_path = "tbbscraper_tor.log"

        tor_rc_fp = open(self.tor_rc_path, "w")
        tor_rc_fp.write("""\
BandwidthRate  {cf.bandwidth}
BandwidthBurst {cf.bandwidth}
DataDirectory  tbbscraper_tor.data
ContactInfo    if this appears somewhere public something is horribly wrong
Log            notice stdout
HardwareAccel  1
Address        {cf.my_ip}
ExitPolicy     reject *:*
Nickname       {cf.my_nickname}
PublishServerDescriptor 0
AssumeReachable 1
ShutdownWaitLength 10
SOCKSPort 0
BridgeRelay 1
BridgeRecordUsageByCountry 0
DirReqStatistics 0
ExtraInfoStatistics 0
ExtendAllowPrivateAddresses 1
""".format(cf=config))
        for port in sorted(config.clients.values()):
            tor_rc_fp.write("ORPort {ip}:{port} IPv4Only\n"
                            .format(ip=config.my_ip, port=port))
        tor_rc_fp.close()

    def __enter__(self):
        tor_log_fp = open(self.tor_log_path, "a")
        self.proc = subprocess.Popen([self.tor_binary, "-f", self.tor_rc_path],
                                     stdin=devnull,
                                     stdout=tor_log_fp,
                                     stderr=tor_log_fp,
                                     close_fds=True)
        tor_log_fp.close()
        return self

    def __exit__(self, *dontcare):
        self.proc.send_signal(signal.SIGINT)
        self.proc.wait()

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

        self.batch_size = int(self.batch_size)
        self.client_ips = self.client_ips.split()
        self.low_tor_port = int(self.low_tor_port)
        self.high_tor_port = self.low_tor_port + len(self.client_ips) - 1
        self.clients = { ip : port
                         for ip, port in zip(self.client_ips,
                                             range(self.low_tor_port,
                                                   self.high_tor_port + 1)) }

def chunk_seq(iterable, size):
    it = iter(iterable)
    while True:
        item = list(itertools.islice(it, size))
        if not item: break
        yield item

cfg = Config("controller.ini")
with open(cfg.batch_list, "rU") as f:
    urls = [l.strip() for l in f]
with TorRelay(cfg):
    time.sleep(5)
    clients = [Client(ip, cfg.my_ip, port, cfg.my_nickname, cfg.my_family)
               for ip, port in cfg.clients.items()]

    nclients = len(clients)
    i = 0
    for block in chunk_seq(urls, cfg.batch_size):
        clients[i % nclients].worker_chan.send(block)
        i += 1

    for cl in clients:
        cl.worker_chan.send([])
        cl.worker_chan.waitclose()
