#! /usr/bin/python3

import base64
import contextlib
import io
import json
import os
import pprint
import subprocess
import sys
import tempfile
import zlib

class CaptureBatchError(subprocess.SubprocessError):
    def __init__(self, returncode, cmd, output=None, stderr=None):
        self.returncode = returncode
        self.cmd = cmd
        self.output = output
        self.stderr = stderr
    def __str__(self):
        if self.returncode:
            text = ("Command '{}' returned non-zero exit status {}"
                    .format(self.cmd, self.returncode))
            if self.output is not None or self.stderr is not None:
                text += " and unexpected output"
        else:
            text = ("Command '{}' exited with unexpected output"
                    .format(self.cmd))
        if self.output is not None:
            text += "\nstdout:\n"
            text += textwrap.indent(self.output, "| ", lambda line: True)
        if self.stderr is not None:
            text += "\nstderr:\n"
            text += textwrap.indent(self.stderr, "| ", lambda line: True)
        return text

class ZlibRawInput(io.RawIOBase):
    def __init__(self, fp):
        self.fp = fp
        self.z  = zlib.decompressobj()
        self.data = bytearray()

    def __fill(self, nbytes):
        if self.z is None:
            return
        while len(self.data) < nbytes:
            data = self.fp.read(16384)
            if data:
                self.data.extend(self.z.decompress(data))
            else:
                self.data.extend(self.z.flush())
                self.fp.close()
                self.z = None # end of file reached
                break

    def readinto(self, buf):
        want = len(buf)
        self.__fill(want)
        have = len(self.data)

        if want <= have:
            buf[:] = self.data[:want]
            del self.data[:want]
            return want
        else:
            buf[:have] = self.data
            del self.data[:have]
            return have

    def readable(self):
        return True

def ZlibTextInput(fp, encoding="utf-8"):
    return io.TextIOWrapper(io.BufferedReader(ZlibRawInput(fp)),
                            encoding=encoding)

def recompress_image(img):
    # this is the base64 encoding of the first six bytes of the PNG signature
    if img.startswith("iVBORw0KG"):
        img = base64.b64decode(img, validate=True)
    if not img.startswith(b"\x89PNG\x0d\x0a\x1a\x0a"):
        raise ValueError("not a PNG image")

    with tempfile.NamedTemporaryFile(suffix=".png") as oldf:
        oldf.write(img)
        oldf.flush()

        # infuriatingly, optipng cannot be told to write *into* a file
        # that already exists; it will always do the rename-out-of-the-way
        # thing.
        newname = oldf.name.replace(".png", "_n.png")
        try:
            output = subprocess.check_output(
                [ "optipng", "-q", "-zc9", "-zs0,1,3", "-f0-5",
                  "-clobber", "-out", newname, oldf.name ],
                stdin=subprocess.DEVNULL,
                stderr=subprocess.STDOUT)
            if output:
                raise CaptureBatchError(0, "optipng", output=output)

            with open(newname, "rb") as newf:
                return newf.read()

        finally:
            with contextlib.suppress(FileNotFoundError):
                os.remove(newname)

def main():
    url_list = zlib.compress("\n".join(sys.argv[1:]).encode("ascii"))
    proc = subprocess.Popen(
        ["python", "planetlab-worker.py"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE)

    proc.stdin.write(url_list)
    proc.stdin.close()

    def recompress_result(obj):
        if 'content' in obj:
            obj['content'] = zlib.compress(obj['content'].encode('utf-8'))
        if 'render' in obj:
            obj['render'] = recompress_image(obj['render'])
        return obj

    results = json.load(ZlibTextInput(proc.stdout),
                        object_hook=recompress_result)
    rc = proc.wait()
    if rc:
        raise subprocess.CalledProcessError(rc, "planetlab-worker")

    #for i, r in enumerate(results):
    #    with open("{:04}.png".format(i), "wb") as f:
    #        f.write(r['render'])
    pprint.pprint(results)

main()
