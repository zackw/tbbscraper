# Arabic (and Farsi) segmentation based on Stanford NLP.

import fcntl
import os
import re
import select
import subprocess
import unicodedata
from collections import deque

PKGDIR        = os.path.dirname(__file__)
SEGMENTER_JAR = os.path.join(PKGDIR, "stanford-segmenter-3.5.2.jar")
ARABIC_DATA   = os.path.join(PKGDIR, "arabic-segmenter-atb+bn+arztrain.ser.gz")
CHINESE_DATA  = os.path.join(PKGDIR, "chinese")

# This constant isn't in os.
PIPE_BUF = 4096

# It may be necessary to convert .xz to .gz files on the fly.  (This
# is for cases where the .gz file would be so big that Github won't
# take it.)
def _recompress_to_gz(xz, gz):
    import lzma
    import gzip
    with lzma.open(xz) as xzf, gzip.open(gz, mode='xb') as gzf:
        while True:
            block = xzf.read(1024 * 1024)
            if not block: break
            gzf.write(block)

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

class Segmenter:
    def __init__(self):
        self._presegment_re = self._get_presegment_re()

        self._proc = subprocess.Popen(
            self.SEGMENTER_INVOCATION,
            stdin  = subprocess.PIPE,
            stdout = subprocess.PIPE,
            stderr = subprocess.DEVNULL)

        _set_nonblocking(self._proc.stdin)
        _set_nonblocking(self._proc.stdout)


    def __del__(self):
        self._proc.terminate()
        self._proc.wait()

    def segment(self, text):
        presegmented = deque()
        if isinstance(text, str):
            text = [text]
        for block in text:
            for token in self._presegment_re.finditer(block):
                presegmented.extend(token.group(0).split())

        # Sentinel value to tell us when we're done with this block of
        # text.  This has to pass through the segmenter proper
        # unmolested, and also has to be something extraordinarily
        # unlikely to appear in the text itself.  U+FDD0 is an
        # official permanent noncharacter.
        presegmented.append("\uFDD0")
        while presegmented:
            _write_many_chunks(self._proc.stdin, presegmented)
            result = _read_all_available(self._proc.stdout)
            for word in result.split():
                if word == "\uFDD0":
                    return
                yield word

        while True:
            result = _read_all_available(self._proc.stdout)
            for word in result.split():
                if word == "\uFDD0":
                    return
                yield word

class ArabicSegmenter(Segmenter):
    SEGMENTER_INVOCATION = [
        "java", "-Xmx1g", "-XX:-UsePerfData", "-classpath", SEGMENTER_JAR,
        "edu.stanford.nlp.international.arabic.process.ArabicSegmenter",
        "-loadClassifier", ARABIC_DATA
    ]

    _PRESEGMENT_RE = None
    @classmethod
    def _get_presegment_re(cls):
        """Our Arabic corpus has a lot of words inexplicably run together,
           which this segmenter doesn't handle (it's more of a
           stemmer, really); we pre-split them using a heuristic:

           split on all nonword characters
           split at the boundary between Arabic and non-Arabic word characters
           split immediately before every occurrence of '⁨ال⁩' (U+0627 U+0644 -
           applying NFKC ensures we don't have to worry about any other form)
        """
        if cls._PRESEGMENT_RE is None:
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

            cls._PRESEGMENT_RE = re.compile(r"""
                  \W+      # one or more nonword characters
                | {A}+     # one or more non-Arabic word characters
                | (?:{al})?(?:(?!{al}){a})*
                           # optional leading 'al', then zero or more
                           # Arabic word characters up to but not
                           # including another 'al'
            """.format(a=arabic, A=not_arabic, al="\u0627\u0644"), re.VERBOSE)

        return cls._PRESEGMENT_RE


class ChineseSegmenter(Segmenter):
    SEGMENTER_INVOCATION = [
        "java", "-Xmx2g", "-classpath", SEGMENTER_JAR,
        "edu.stanford.nlp.ie.crf.CRFClassifier",
        "-inputEncoding", "utf8", "-readStdin",
        "-sighanCorporaDict", CHINESE_DATA,
        "-loadClassifier", os.path.join(CHINESE_DATA, "ctb.gz"),
        "-serDictionary", os.path.join(CHINESE_DATA, "dict-chris6.ser.gz")
    ]

    def __init__(self):
        # ctb.gz is very large - too large for Github.  So it's actually in
        # the repo in .xz format and we convert it on the fly.
        ctb_gz = os.path.join(CHINESE_DATA, "ctb.gz")
        ctb_xz = os.path.join(CHINESE_DATA, "ctb.xz")
        if not os.path.exists(ctb_gz):
            if not os.path.exists(ctb_xz):
                raise RuntimeError("neither {} nor {} exists"
                                   .format(ctb_gz, ctb_xz))
            _recompress_to_gz(ctb_xz, ctb_gz)

        Segmenter.__init__(self)

    _PRESEGMENT_RE = None
    @classmethod
    def _get_presegment_re(cls):
        """For Chinese, the segmenter is doing much more of the real work,
           but a similar heuristic to that used for Arabic is still useful:

           split on all nonword characters
           split at the boundary between Chinese and non-Chinese word
             characters

           We also include a backstop, since the base class will not
           tolerate more than PIPE_BUF _bytes_ per presegmented item.
        """
        if cls._PRESEGMENT_RE is None:
            # Partition the set of characters matched by \w into Chinese
            # and non-Chinese
            w = re.compile("\A\w\Z")
            chinese = set()
            not_chinese = set()
            for x in range(0, 0x10FFFF):
                c = chr(x)
                if w.match(c):
                    # Chinese Unicode blocks (approximately)
                    if (0x002E80 <= x <= 0x002EFF or # CJK Radicals Suppl.
                        0x002F00 <= x <= 0x002FDF or # Kangxi Radicals
                        0x003000 <= x <= 0x00303F or # CJK Symbols and Punct.
                        0x003200 <= x <= 0x004DBF or # CJK Compat/ExtA
                        0x004E00 <= x <= 0x009FFF or # CJK Unified
                        0x00F900 <= x <= 0x00FAFF or # CJK Compat
                        0x020000 <= x <= 0x02FFFF):  # SIP
                        chinese.add(c)
                    else:
                        not_chinese.add(c)

            chinese     = "[" + "".join(sorted(chinese)) +     "]"
            not_chinese = "[" + "".join(sorted(not_chinese)) + "]"

            cls._PRESEGMENT_RE = re.compile(r"""
                  \W{{1,512}}  # up to 512 nonword characters
                | {C}{{1,512}} # up to 512 non-Chinese word characters
                | {c}{{1,512}} # up to 512 Chinese word characters
            """.format(c=chinese, C=not_chinese), re.VERBOSE)

        return cls._PRESEGMENT_RE
