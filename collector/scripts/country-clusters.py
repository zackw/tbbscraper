#! /usr/bin/python3

import collections
import functools
import os
import re
import subprocess
import sys

import GeoIP
gi = GeoIP.open("/usr/share/GeoIP/GeoIPCity.dat", GeoIP.GEOIP_STANDARD)

def get_country(ipaddr):
    if ipaddr[0] == "*":
        return "[unknown IP]"
    try:
        gir = gi.record_by_addr(ipaddr)
        if gir: return gir['country_name']
    except:
        pass
    return "[unknown location]"

def int_to_base36(num):
    """Converts a positive integer into a base36 string."""
    assert num >= 0
    digits = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'

    res = ''
    while not res or number > 0:
        number, i = divmod(num, 36)
        res = digits[i] + res
    return res

def all_paths(data, start):
    path = []

    def all_paths_r(data, start):
        path.append(start)
        dests = data[start]
        if dests:
            for d in dests:
                yield from all_paths_r(data, d)
        else:
            yield path[:]
        path.pop()

    return list(all_paths_r(data, start))


class Node:
    _counter = 1
    _nodes = {}
    def __new__(cls, ipaddr):
        if ipaddr in cls._nodes:
            return cls._nodes[ipaddr]

        name = 'n' + int_to_base36(cls._counter)
        cls._counter += 1

        rv = super(Node, cls).__new__(cls)
        rv.ipaddr  = ipaddr
        rv.name    = name
        rv.country = get_country(ipaddr)
        if len(rv.country) > 20:
            rv.country = rv.country[:18].strip() + '…' + rv.country[-1];

        cls._nodes[ipaddr] = rv
        return rv

    def __hash__(self):
        return hash(self.ipaddr)

    def write_nodedef(self, fp):
        if ipaddr[0] == "*":
            fp.write('{} [ shape=circle,label="" ];\n'
                     .format(self.name))
        else:
            fp.write('{} [ shape=box,label="{}" ];\n'
                     .format(self.name,
                             self.ipaddr + r"\n" + self.country))

class Graph:
    new_trace_re = re.compile(r"^tracelb from ([0-9.]+) to ([0-9.]+),")

    def __init__(self):
        self.paths = set()
        self.servers = {}

    def process_wf(self, wf):
        srcname = os.path.basename(wf).partition('.hma')[0]
        with subprocess.Popen(["sc_warts2text", wf],
                              stdin  = subprocess.DEVNULL,
                              stdout = subprocess.PIPE) as proc:
            self.process_decoded(srcname, proc.stdout)

    def process_decoded(self, srcname, inf):
        me = None
        dest = None
        depth = 0
        edges = collections.defaultdict(set)
        for line in inf:
            line = line.decode("ascii")
            m = self.new_trace_re.match(line)
            if m:
                if me is not None:
                    self.finalize_traceset(edges, me, dest)
                    edges.clear()
                depth = 0
                me = m.group(1)
                dest = m.group(2)
                if me not in self.servers:
                    self.servers[me] = srcname
                continue

            assert me is not None
            hops = line.strip().split(" -> ")
            assert len(hops) >= 2
            if depth == 0:
                if hops[0] == '*':
                    hops[0] = me

            for i in range(len(hops)-1):
                f, t = hops[i], hops[i+1]
                if f[0] == '(':
                    f = f[1:-1].split(", ")
                else:
                    f = [f]
                if t[0] == '(':
                    t = t[1:-1].split(", ")
                else:
                    t = [t]

                for ff in f:
                    if ff == "*": ff += str(depth + i)
                    for tt in t:
                        if tt == "*": tt += str(depth + i + 1)
                        edges[ff].add(tt)

            depth += len(hops) - 1

    def finalize_traceset(self, edges, me, dest):
        paths = all_paths(edges, me)
        for path in paths:
            path.append(dest)
            self.paths.add(tuple(path))

    def dump(self):
        for path in self.paths:
            print(repr(path))

def main():
    graph = Graph()
    for wf in sys.argv[1:]:
        graph.process_wf(wf)
    graph.dump()

main()
