from . import dongdu
from . import pythai
from . import stanford

import MeCab

from collections import defaultdict
import regex as re
import unicodedata

__all__ = ('segment', 'presegment', 'is_nonword', 'is_url')

# https://gist.github.com/gruber/8891611
URL_RE = r"""
(?xi)
\b
(							# Capture 1: entire matched URL
  (?:
    https?:				# URL protocol and colon
    (?:
      /{1,3}						# 1-3 slashes
      |								#   or
      [a-z0-9%]						# Single letter or digit or '%'
      								# (Trying not to match e.g. "URI::Escape")
    )
    |							#   or
    							# looks like domain name followed by a slash:
    [a-z0-9.\-]+[.]
    (?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj| Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)
    /
  )
  (?:							# One or more:
    [^\s()<>{}\[\]]+						# Run of non-space, non-()<>{}[]
    |								#   or
    \([^\s()]*?\([^\s()]+\)[^\s()]*?\)  # balanced parens, one level deep: (…(…)…)
    |
    \([^\s]+?\)							# balanced parens, non-recursive: (…)
  )+
  (?:							# End with:
    \([^\s()]*?\([^\s()]+\)[^\s()]*?\)  # balanced parens, one level deep: (…(…)…)
    |
    \([^\s]+?\)							# balanced parens, non-recursive: (…)
    |									#   or
    [^\s`!()\[\]{};:'".,<>?«»“”‘’]		# not a space or one of these punct chars
  )
  |					# OR, the following to match naked domains:
  (?:
  	(?<!@)			# not preceded by a @, avoid matching foo@_gmail.com_
    [a-z0-9]+
    (?:[.\-][a-z0-9]+)*
    [.]
    (?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj| Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)
    \b
    /?
    (?!@)			# not succeeded by a @, avoid matching "foo.na" in "foo.na@example.com"
  )
)
"""

class Segmenter:
    """Segmenter is a singleton object which does lazy initialization of
       the various external segmenters, some of which are quite
       expensive to start up.  It provides methods to "presegment" and
       "segment" text, and also has utility methods to identify URLs
       and nonwords (nonwords consist entirely of digits and punctuation).
    """

    def __init__(self):
        symbols_s = []
        symbols_t = []
        digits    = []
        white     = []
        for c in range(0x10FFFF):
            x = chr(c)
            cat = unicodedata.category(x)
            if cat[0] in ('P', 'S'): # Punctuation, Symbols
                # These symbol characters may appear inside a word without
                # breaking it in two.  FIXME: Any others?
                split = (x not in ('-', '‐', '\'', '’', '.'))

                # These characters need to be escaped inside a character class.
                if (x in '\\', '[', ']', '-'):
                    x = '\\' + x

                symbols_t.append(x)
                if split:
                    symbols_s.append(x)

            elif cat[0] == 'N':
                digits.append(x)

            # Treat all C0 and C1 controls the same as whitespace.
            # (\t\r\n\v\f are *not* in class Z.)
            elif cat[0] == 'Z' or cat in ('Cc', 'Cf'):
                white.append(x)

        symbols_s = "".join(symbols_s)
        symbols_t = "".join(symbols_t)
        digits    = "".join(digits)
        white     = "".join(white)

        self.white      = re.compile("["  +             white +          "]+")
        self.split      = re.compile("["  + symbols_s + white +          "]+")
        self.nonword    = re.compile("^[" + symbols_t + white + digits + "]+$")
        self.left_trim  = re.compile("^[" + symbols_t + white +          "]+")
        self.right_trim = re.compile("["  + symbols_t + white +          "]+$")

        self.url = re.compile(URL_RE, re.VERBOSE|re.IGNORECASE)

        self.mecab      = None
        self.dongdu     = None
        self.s_chinese  = None
        self.s_arabic   = None
        self._lang_segmenters = defaultdict(
            lambda: self._lang_segment_default, {
                'zh':      self._lang_segment_zh,
                'zh-Hant': self._lang_segment_zh,
                'ja':      self._lang_segment_ja,
                'vi':      self._lang_segment_vi,
                'th':      self._lang_segment_th,

                # The Arabic segmenter is trained on the _language_,
                # not the _script_, but should still do acceptably
                # well (better than the generic, anyway) on the other
                # common languages written with that script.
                'ar':      self._lang_segment_ar,
                'fa':      self._lang_segment_ar,
                'ku':      self._lang_segment_ar,
                'ps':      self._lang_segment_ar,
                'ur':      self._lang_segment_ar
            })

    # Public entry points:
    def is_url(self, text):
        """If TEXT contains an URL, return that URL. Otherwise, return None."""
        m = self.url.match(text)
        if m: return m.group(1)
        return None

    def is_nonword(self, text):
        """True if TEXT consists entirely of digits and punctuation."""
        return bool(self.nonword.match(text))

    def presegment(self, text):
        """Perform generic word segmentation on TEXT.  Returns an iterable."""
        return self._lang_segment_default(text)

    def segment(self, lang, text):
        """Perform language-aware word segmentation on TEXT.
           Returns an iterable."""
        return self._lang_segmenters[lang](text)

    # Internal:
    def _presegment_internal(self, text, language_seg):
        """Presegmentation is independent of language.  It first splits on
           (Unicode) whitespace, then detects embedded URLs which are
           passed through unmodified, and then splits again on punctuation
           and trims a slightly larger set of leading and trailing
           punctuation.  Anything that survives that process is fed to the
           language-specific segmenter.
        """
        for word in self.white.split(text):
            m = self.url.match(word)
            if m:
                yield m.group(1)
            else:
                for w in self.split.split(word):
                    w = self.left_trim.sub("", w)
                    if w:
                        w = self.right_trim.sub("", w)
                        yield from language_seg(
                            unicodedata.normalize('NFKC', w).casefold())

    def _lang_segment_default(self, text):
        """The default behavior is just to do presegmentation."""
        return self._presegment_internal(text, lambda word: (word,))

    # Thai: libthai/pythai (needs no initialization)
    def _lang_segment_th(self, text):
        return self._presegment_internal(
            text, lambda word: pythai.split(word))

    # Japanese: MeCab
    def _lang_segment_ja(self, text):
        if self.mecab is None:
            # '-O wakati' means "put spaces between the words"
            self.mecab = MeCab.Tagger('-O wakati')

        return self._presegment_internal(
            text, lambda word: self.mecab.parse(word).split())

    # Chinese: SNLP
    def _lang_segment_zh(self, text):
        if self.s_chinese is None:
            self.s_chinese = stanford.ChineseSegmenter()
        return self._presegment_internal(
            text, lambda word: self.s_chinese.segment(word))

    # Arabic and related languages: SNLP + heuristics
    def _lang_segment_ar(self, text):
        if self.s_arabic is None:
            self.s_arabic = stanford.ArabicSegmenter()
        return self._presegment_internal(
            text, lambda word: self.s_arabic.segment(word))

    # Vietnamese: dongdu
    # In Vietnamese, spaces appear _within_ every multisyllabic word.
    # Also, the segmenter cares about word capitalization.
    # To handle this correctly we must reimplement the presegmentation
    # loop ourselves.
    def _lang_segment_vi(self, text):
        if self.dongdu is None:
            self.dongdu = dongdu.Segmenter()

        run = []
        def flush():
            nonlocal run
            if run:
                # The output of dongdu.segment may need a second
                # round of trimming.
                for w in self.dongdu.segment(" ".join(run)):
                    w = self.left_trim.sub("", w)
                    if w:
                        yield self.right_trim.sub("", w).casefold()
                run = []

        for word in self.white.split(text):
            m = self.url.match(word)
            if m:
                yield from flush()
                yield m.group(1)
            else:
                for w in self.split.split(word):
                    w = self.left_trim.sub("", w)
                    if w:
                        w = self.right_trim.sub("", w)
                        run.append(unicodedata.normalize('NFKC', w))

        yield from flush()

_segmenter = None
def presegment(text):
    global _segmenter
    if _segmenter is None: _segmenter = Segmenter()
    return _segmenter.presegment(text)

def segment(lang, text):
    global _segmenter
    if _segmenter is None: _segmenter = Segmenter()
    return _segmenter.segment(lang, text)

def presegment_iter(text):
    global _segmenter
    if _segmenter is None: _segmenter = Segmenter()
    return _segmenter.presegment_iter(text)

def is_url(text):
    global _segmenter
    if _segmenter is None: _segmenter = Segmenter()
    return _segmenter.is_url(text)

def is_nonword(text):
    global _segmenter
    if _segmenter is None: _segmenter = Segmenter()
    return _segmenter.is_nonword(text)
