from . import mmseg
from . import tinysegmenter
from . import dongdu
from . import pythai
from . import minipunkt

from collections import defaultdict
import re
import unicodedata

__all__ = ('segment',)

# Japanese: tinysegmenter
_TinySegmenter = None
def _segment_ja(text):
    global _TinySegmenter
    if _TinySegmenter is None:
        _TinySegmenter = tinysegmenter.TinySegmenter()
    return _TinySegmenter.tokenize(text)

# Chinese: mmseg
_mmseg_initialized = False
def _segment_zh(text):
    global _mmseg_initialized
    if not _mmseg_initialized:
        mmseg.dict_load_defaults()
        _mmseg_initialized = True
    return (str(t) for t in mmseg.Algorithm(text))

# Vietnamese: dongdu
_dongdu_segm = None
def _segment_vi(text):
    global _dongdu_segm
    if _dongdu_segm is None:
        _dongdu_segm = dongdu.Segmenter()
    return _dongdu_segm.segment(text)

# Thai: libthai/pythai
def _segment_th(text):
    return pythai.split(text)

# default: minipunkt
_punkt_wt = None
def _segment_default(text):
    global _punkt_wt
    if _punkt_wt is None:
        _punkt_wt = minipunkt.WordTokenizer()
    return _punkt_wt.tokenize(text)

_segmenters = defaultdict(lambda: _segment_default, {
    'zh':      _segment_zh,
    'zh-Hant': _segment_zh,
    'ja':      _segment_ja,
    'vi':      _segment_vi,
    'th':      _segment_th,
})

# Some segmenters leave punctuation and whitespace in the output,
# (as separate strings), others don't.  We don't want any of that.
def _prep_cleanup_re():
    unwanted_chars = []
    for c in range(0x10FFFF):
        x = chr(c)
        cat = unicodedata.category(x)
        # All punctuation, all whitespace, C0 and C1 controls,
        # and "format effectors" (e.g. ZWNJ, RLE).  Cn (unassigned),
        # Cs (surrogate), and Co (private use) are not stripped.
        if cat[0] in ('P', 'Z') or cat in ('Cc', 'Cf'):
            unwanted_chars.append(x)
    return (re.compile("^[" + "".join(unwanted_chars) + "]+"),
            re.compile("[" + "".join(unwanted_chars) + "]+$"))

_left_cleanup_re, _right_cleanup_re = _prep_cleanup_re()

def cleanup_iter(seq):
    for item in seq:
        # Microoptimization: if 'item' is not the empty string after
        # _left_cleanup_re then it will still not be the empty string
        # after _right_cleanup_re and casefold, so we can hoist the
        # empty-string check to immediately after _left_cleanup_re.
        item = _left_cleanup_re.sub("", item)
        if item:
            yield _right_cleanup_re.sub("", item).casefold()


def segment(lang, text):
    """TEXT is believed to be in language LANG (an ISO 639 code); segment
       it into words.  Returns an iterable.
    """
    raw = _segmenters[lang](text)
    return cleanup_iter(raw)
