#! /usr/bin/python3

# Look up the IP addresses of all the hostnames in the file provided
# on standard input, and write them back out to stdout in the form
# <addr> <name>.  <name> is IDNA regardless of the form of the input.
# Also reports the IP addresses of all configured DNS servers.

import asyncio
import re
import socket
import sys

clean_line_re = re.compile(r"^\s*([^#]*?)\s*(?:#.*)?$")
parse_line_re = re.compile(r"^(?P<name>\S+)(?:\s+\((?P<addr>[0-9.]+)\))?$")

def parse_input(fp):
    """Read the list of hostnames to process; returns a list of hostnames
       in canonical form.  Each list entry is either ('hostname', None)
       or ('hostname', 'address'). If 'address' is not None, that
       means HOSTNAME has already been bound to ADDRESS by the creator
       of the list and does not need to be looked up again.
    """
    rv = []
    fail = False
    for i, line in enumerate(fp):
        m = clean_line_re.match(line)
        if m: cline = m.group(1)
        else: cline = line
        if not cline: continue
        m = parse_line_re.match(cline)
        if not m:
            sys.stderr.write("invalid input line {}: {!r}\n"
                             .format(i+1, line))
            fail = True
            continue

        rv.append((m.group('name').encode('idna').decode('ascii'),
                   m.group('addr')))

    if fail:
        raise SystemExit(1)
    return rv

@asyncio.coroutine
def lookup(loop, name, addr):
    """Look up a single IP address.  We only record IPv4 addresses because
       PhantomJS doesn't do IPv6 yet, so all the other testing was done
       with IPv4 only."""
    if addr is None:
        try:
            sys.stderr.write(name + "\n")
            addrs = yield from loop.getaddrinfo(name, 443,
                                                family=socket.AF_INET,
                                                proto=socket.IPPROTO_TCP)
        except Exception as e:
            return [(name, e)]
        return [(name, a[4][0]) for a in addrs]
    else:
        return [(name, addr)]

def lookup_names(names):
    """Look up IP addresses for all requested names, in parallel."""
    loop = asyncio.get_event_loop()
    tasks = [asyncio.async(lookup(loop, name, addr), loop=loop)
             for name, addr in names]
    results = loop.run_until_complete(asyncio.gather(*tasks, loop=loop))
    for r in results:
        for name, addr in r:
            if isinstance(addr, OSError):
                addr = "X:" + addr.strerror
            elif isinstance(addr, Exception):
                addr = "X:" + str(addr)
            else:
                assert isinstance(addr, str)
            sys.stdout.write("{} {}\n".format(name, addr))

def get_dns_servers():
    """Report all the configured name servers (under the pseudo-name
       "nameserver").  As above, IPv4 addresses only."""
    with open("/etc/resolv.conf") as f:
        for line in f:
            if line.startswith("nameserver ") and ':' not in line:
                sys.stdout.write(line)

def main():
    names = parse_input(sys.stdin)
    lookup_names(names)
    get_dns_servers()

main()
