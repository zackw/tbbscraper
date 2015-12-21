#! /usr/bin/python3

import subprocess
import sys
import os
import re
import functools
from collections import defaultdict

def toposort2(data):
    # Ignore self dependencies.
    for k, v in data.items():
        v.discard(k)
    # Find all items that don't depend on anything.
    extra_items_in_deps = functools.reduce(set.union, data.values()) - set(data.keys())
    # Add empty dependences where needed
    data.update({item:set() for item in extra_items_in_deps})
    while True:
        ordered = set(item for item, dep in data.items() if not dep)
        if not ordered:
            break
        for o in sorted(ordered): yield o
        data = {item: (dep - ordered)
                for item, dep in data.items()
                    if item not in ordered}

def squeeze_repeats(seq):
    rv = []
    prev = None
    for s in seq:
        if s != prev:
            rv.append(s)
            prev = s
    rv.reverse()
    return rv

import GeoIP
GI = GeoIP.open("/usr/share/GeoIP/GeoIPCity.dat", GeoIP.GEOIP_STANDARD)
@functools.lru_cache(maxsize=1024)
def get_country(ipaddr):
    if ipaddr == "<origin>":
        return "<origin>"
    if ipaddr == "*":
        return "[unknown IP]"
    try:
        gir = GI.record_by_addr(ipaddr)
        if gir: return gir['country_name']
    except:
        pass
    return "[unknown location]"

new_trace_re = re.compile(r"^tracelb from ([0-9.]+) to ([0-9.]+),")

def process_decoded(inf, infname):
    destinations = {}
    current_trace = None
    current_dest = None
    me = None
    for line in inf:
        line = line.decode("ascii")
        m = new_trace_re.match(line)
        if m:
            if current_trace is not None:
                assert current_dest is not None
                destinations[current_dest] = squeeze_repeats(get_country(x) for x in toposort2(current_trace))
            me = m.group(1)
            current_dest = m.group(2)
            current_trace = defaultdict(set)
        else:
            hops = line.strip().split(" -> ")
            assert len(hops) >= 2

            if me:
                if hops[0] == '*':
                    hops[0] = '<origin>'
                me = None

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
                    for tt in t:
                        current_trace[ff].add(tt)

    if current_trace is not None:
        assert current_dest is not None
        destinations[current_dest] = squeeze_repeats(get_country(x) for x in toposort2(current_trace))

    junk = set()
    for tag, trace in destinations.items():
        if trace and trace[0] == '<origin>':
            del trace[0]
        if trace and trace[0] == 'Anonymous Proxy':
            del trace[0]
        if not trace:
            junk.add(tag)

    for tag in junk:
        del destinations[tag]

    prefixes = set()
    traces = sorted(destinations.values())
    for a, b in zip(traces[:-1], traces[1:]):
        p = os.path.commonprefix([a, b])
        if p: prefixes.add(tuple(p))

    infname = os.path.splitext(os.path.basename(infname))[0]
    if not prefixes:
        sys.stdout.write("{}\t[no data]\n".format(infname))
    else:
        for p in sorted(prefixes):
            sys.stdout.write("{}\t{}\n".format(infname, ", ".join(p)))

def process_wf(wf):
    with subprocess.Popen(["sc_warts2text", wf],
                          stdin  = subprocess.DEVNULL,
                          stdout = subprocess.PIPE) as proc:
        process_decoded(proc.stdout, wf)

for wf in sys.argv[1:]: 
    process_wf(wf)
