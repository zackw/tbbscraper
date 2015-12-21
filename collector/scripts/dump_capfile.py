#! /usr/bin/python3

import sys
import pprint
import json
import zlib

def dump_one(fname):
    with open(fname, "rb") as f:
        magic = f.read(8)
        if magic != b'\x7fcap 00\n':
            sys.stdout.write("{}: not a capture file\n\n".format(fname))
            return

        data = memoryview(f.read())

    b1 = data.obj.find(b'\n')
    b2 = data.obj.find(b'\n', b1+1)
    b3 = data.obj.find(b'\n', b2+1)
    b4 = data.obj.find(b'\n', b3+1)
    b5 = data.obj.find(b'\n', b4+1)
    b6 = data.obj.find(b'\n', b5+1)

    url  = data[     : b1].tobytes().decode("utf-8")
    rurl = data[b1+1 : b2].tobytes().decode("utf-8")
    stat = data[b2+1 : b3].tobytes().decode("utf-8")
    dtyl = data[b3+1 : b4].tobytes().decode("utf-8")
    elap = float(data[b4+1 : b5].tobytes().decode("utf-8"))
    lens = data[b5+1 : b6].tobytes().decode("ascii").split()
    clen = int(lens[0])
    llen = int(lens[1])

    sys.stdout.write("{}:\n"
                     "   URL  {}\n"
                     "  RURL  {}\n"
                     "  STAT  {}\n"
                     "  DTYL  {}\n"
                     "  ELAP  {:.6f}\n"
                     "  CLEN  {}\n"
                     "  LLEN  {}\n"
                     "  -- HTML --\n"
                     .format(fname, url, rurl, stat, dtyl, elap, clen, llen))

    cbeg = b6 + 1
    cend = cbeg + clen
    lbeg = cend
    lend = lbeg + llen
    assert lend == len(data)
    sys.stdout.write(zlib.decompress(data[cbeg:cend]).decode("utf-8"))
    sys.stdout.write("\n  -- LOG --\n")
    pprint.pprint(json.loads(zlib.decompress(data[lbeg:lend]).decode("utf-8")))
    sys.stdout.write("\n")

def main():
    for arg in sys.argv[1:]:
        dump_one(arg)

main()

