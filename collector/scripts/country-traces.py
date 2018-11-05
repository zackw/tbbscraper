#! /usr/bin/python3

import subprocess
import sys
import os
import re
import functools

import GeoIP
gi = GeoIP.open("/usr/share/GeoIP/GeoIPCity.dat", GeoIP.GEOIP_STANDARD)

@functools.lru_cache(maxsize=1024)
def get_city(ipaddr):
    if ipaddr == "*":
        return "[unknown IP]"
    try:
        gir = gi.record_by_addr(ipaddr)
        if gir: return gir['country_name']
    except:
        pass
    return "[unknown location]"

new_trace_re = re.compile(r"^tracelb from ([0-9.]+) to ([0-9.]+),")

def finish_trace(outf):
    outf.close()

def start_trace(outd, destip):
    return open(os.path.join(outd, destip), "wt")

def process_decoded(inf, outd, gi):
    outf = None
    me = None
    for line in inf:
        line = line.decode("ascii")
        m = new_trace_re.match(line)
        if m:
            if outf is not None:
                finish_trace(outf)
            outf = start_trace(outd, m.group(2))
            me = m.group(1)
        else:
            assert outf
            hops = line.strip().split(" -> ")
            assert len(hops) >= 2

            if me:
                if hops[0] == '*':
                    hops[0] = me
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
                        outf.write("{!r} -> {!r};\n".format(
                            get_city(ff),
                            get_city(tt)))
    if outf is not None:
        finish_trace(outf)

def process_wf(wf, gi):
    outd = os.path.splitext(wf)[0] + ".ct"
    os.makedirs(outd, exist_ok=True)
    with subprocess.Popen(["sc_warts2text", wf],
                          stdin  = subprocess.DEVNULL,
                          stdout = subprocess.PIPE) as proc:
        process_decoded(proc.stdout, outd, gi)


for wf in sys.argv[1:]:
    process_wf(wf, gi)
