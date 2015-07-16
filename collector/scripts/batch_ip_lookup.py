#! /usr/bin/python3

# Look up the IP addresses of all the hostnames in the file provided
# on standard input, and write them back out to stdout in the form
# <addr> <name>.  <name> is IDNA regardless of the form of the input.
# Optionally reports the IP addresses of all configured DNS servers.

import re
import sys
import time

from _dnslookup import getaddrinfo_batch

from itertools import zip_longest
def chunked(iterable, n):
    args = [iter(iterable)]*n
    return zip_longest(*args)

clean_line_re = re.compile(r"^\s*([^#]*?)\s*(?:#.*)?$")
parse_line_re = re.compile(r"^(?P<name>\S+)(?:\s+\((?P<addr>[0-9.]+)\))?$")

def parse_input(fp):
    """Read the list of hostnames to process; returns a list of hostnames
       in canonical form.  Each list entry is either ('hostname', None)
       or ('hostname', 'address'). If 'address' is not None, that
       means HOSTNAME has already been bound to ADDRESS by the creator
       of the list and does not need to be looked up again.
    """
    names = set()
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

        name = m.group('name').encode('idna').decode('ascii')
        addr = m.group('addr')

        if '.' not in name or '..' in name or name.endswith('.'):
            sys.stderr.write("invalid DNS name on line {}: {!r}\n"
                             .format(i+1, name))

        names.add((name, addr))

    if fail:
        return []

    # Sort the list by suffix; this means we will look up every entry
    # in a particular domain all at once, maximizing DNS cache efficiency.
    return sorted(names, key = lambda v: list(reversed(v[0].split('.'))))

def lookup_names(resolver, names):
    """Look up IP addresses for all requested names."""

    # glibc's getaddrinfo_a has a hardwired undocumented assumption
    # that you will only ask for 64 names at a time.
    count = 0
    for block in chunked(names, 64):
        eblock = [n.encode("ascii") for n in block if n is not None]
        count += len(eblock)
        eresults = getaddrinfo_batch(eblock)
        sys.stderr.write(count + "\n")
        sys.stderr.flush()
        for ename, addrs in eresults:
            name = ename.decode("ascii")
            if isinstance(addrs, OSError):
                sys.stdout.write("{} X:{}\n".format(name, addrs.strerror))
            elif isinstance(addrs, Exception):
                sys.stdout.write("{} X:{}\n".format(name, str(addrs)))
            else:
                for addr in addrs:
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
    if not names:
        sys.exit(1)

    if len(sys.argv) > 1:
        resolver = dns.resolver.Resolver(configure=False)
        resolver.nameservers = sys.argv[1:]
        lookup_names(resolver, names)
    else:
        lookup_names(dns.resolver.Resolver(), names)
        get_dns_servers()

if __name__ == '__main__':
    main()
