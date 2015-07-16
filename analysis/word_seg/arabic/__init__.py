# Arabic (and Farsi) segmentation based on Stanford NLP.

import fcntl
import os
import re
import select
import subprocess
import unicodedata
from collections import deque

SEGMENTER_JAR = "stanford-segmenter-3.4.1.jar"
SEGMENTER_DAT = "arabic-segmenter-atb+bn+arztrain.ser.gz"

# This constant isn't in os.
PIPE_BUF = 4096

def _set_nonblocking(f):
    flags = fcntl.fcntl(f, fcntl.F_GETFL)
    fcntl.fcntl(f, fcntl.F_SETFL, (flags | os.O_NONBLOCK))

def _read_all_available(f):
    """Read all data currently available from F, which is a nonblocking
       file descriptor (or a filelike object with a .fileno method).
       If _no_ data is currently available, blocks until some is.
       Only returns the empty string upon hitting EOF.
    """
    if not isinstance(f, int):
        f = f.fileno()

    poll = select.poll()
    poll.register(f, select.POLLIN|select.POLLHUP)
    events = poll.poll()
    assert len(events) == 1
    assert events[0][0] == f
    if events[0][1] == select.POLLHUP:
        raise subprocess.CalledProcessError("Stanford NLP segmenter has exited")

    chunks = []
    while True:
        try:
            c = os.read(f, PIPE_BUF)
        except BlockingIOError:
            break
        if not c:
            break
        chunks.append(c)

    if not chunks:
        raise subprocess.CalledProcessError("Stanford NLP segmenter has exited")

    return b"".join(chunks).decode("utf-8")

def _write_many_chunks(f, q):
    """F is a nonblocking file descriptor for a pipe (or a filelike with a
       .fileno method, ditto).  Q is a deque.  Write as many items as
       possible (until write() would block) from the deque into the
       pipe, separated by newline characters. Each item must be less
       than PIPE_BUF-1 bytes after encoding (this ensures that it,
       plus the separator, is written _atomically_ into the pipe).
    """
    if not isinstance(f, int):
        f = f.fileno()

    try:
        while q:
            item = q[0]
            if not isinstance(item, bytes):
                item = item.encode('utf-8')
            item += b"\n"
            if len(item) > PIPE_BUF:
                raise RuntimeError("Item '{!r}' too long for the pipe"
                                   .format(item))
            os.write(f, item)
            q.popleft()

    except BlockingIOError:
        return

# Our Arabic corpus has a lot of words inexplicably run
# together, which this segmenter doesn't handle (it's more of
# a stemmer, really); we pre-split them using a heuristic:
# split on all nonword characters
# split at the boundary between Arabic and non-Arabic word characters
# split immediately before every occurrence of '⁨ال⁩' (U+0627 U+0644 -
# applying NFKC ensures we don't have to worry about any other form)

_presegment_re = None
def _get_presegment_re():
    global _presegment_re
    if _presegment_re is None:
        # Partition the set of characters matched by \w into Arabic
        # and non-Arabic.
        w = re.compile("\A\w\Z")
        arabic = set()
        not_arabic = set()
        for x in range(0, 0x10FFFF):
            c = chr(x)
            if w.match(c):
                # Arabic Unicode blocks.  Incoming text is already
                # NFKC so we shouldn't have to worry about combining
                # marks outside these ranges (and maybe not the
                # presentation forms either, but let's be cautious)
                if (0x000600 <= x <= 0x0006FF or
                    0x000750 <= x <= 0x00077F or
                    0x0008A0 <= x <= 0x0008FF or
                    0x00FB50 <= x <= 0x00FDFF or
                    0x00FE70 <= x <= 0x00FEFF or
                    0x010E60 <= x <= 0x010E7F or
                    0x01EE00 <= x <= 0x01EEFF):
                    arabic.add(c)
                else:
                    not_arabic.add(c)

        arabic     = "[" + "".join(sorted(arabic)) +     "]"
        not_arabic = "[" + "".join(sorted(not_arabic)) + "]"

        _presegment_re = re.compile(r"""
              \W+      # one or more nonword characters
            | {A}+     # one or more non-Arabic word characters
            | (?:{al})?(?:(?!{al}){a})*
                       # optional leading 'al', then zero or more Arabic
                       # word characters up to but not including another 'al'
        """.format(a=arabic, A=not_arabic, al="\u0627\u0644"), re.VERBOSE)

    return _presegment_re

class Segmenter:
    def __init__(self):
        pkgdir = os.path.dirname(__file__)
        segmenter_jar = os.path.join(pkgdir, SEGMENTER_JAR)
        segmenter_dat = os.path.join(pkgdir, SEGMENTER_DAT)

        self.proc = subprocess.Popen(
            ["java", "-Xmx1g", "-classpath", segmenter_jar,
             "edu.stanford.nlp.international.arabic.process.ArabicSegmenter",
             "-loadClassifier", segmenter_dat],
            stdin  = subprocess.PIPE,
            stdout = subprocess.PIPE,
            stderr = subprocess.DEVNULL)

        _set_nonblocking(self.proc.stdin)
        _set_nonblocking(self.proc.stdout)

        self._presegment_re = _get_presegment_re()

    def __del__(self):
        self.proc.terminate()
        self.proc.wait()

    def segment(self, text):
        presegmented = deque()
        for token in self._presegment_re.finditer(text):
            presegmented.extend(token.group(0).split())
     
        # Sentinel value to tell us when we're done with this block of
        # text.  This has to pass through the segmenter proper
        # unmolested, and also has to be something extraordinarily
        # unlikely to appear in the text itself.  U+FDD0 is an
        # official permanent noncharacter.
        presegmented.append("\uFDD0")
        while presegmented:
            _write_many_chunks(self.proc.stdin, presegmented)
            result = _read_all_available(self.proc.stdout)
            for word in result.split():
                if word == "\uFDD0":
                    return
                yield word

        while True:
            result = _read_all_available(self.proc.stdout)
            for word in result.split():
                if word == "\uFDD0":
                    return
                yield word

