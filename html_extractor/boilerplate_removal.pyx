# Boilerplate removal from HTML documents.
# This is a hybrid of two approaches, both from ACM SIGIR:
#
# * Shuang Lin, Jie Chen, Zhendong Niu
#   "Combining a Segmentation-Like Approach and A Density-Based
#   Approach in Content Extraction"
#   http://ieeexplore.ieee.org/xpls/abs_all.jsp?arnumber=6216755&tag=1
# * Fei Sun, Dandan Song, Lejian Lao
#   "DOM Based Content Extraction via Text Density"
#   http://disnet.cs.bit.edu.cn/DOM%20Based%20Content%20Extraction%20via%20Text%20Density.pdf
#
# Compared to other approaches in the literature, these algorithms
# share the critical advantage (for our purposes) of being completely
# language-neutral, and the secondary advantage of not relying much on
# the official semantics of HTML elements.  The latter also avoids any
# reliance on manually tuned parameters.
#
# Unfortunately, neither algorithm is well described; the papers
# contain critical ambiguities and underspecifications
# (e.g. incomplete lists of HTML elements considered to be "block
# level" or "basic text") and possibly outright errors (e.g. the
# formula for "composite text density" in paper #2 has no rationale
# and is baroque to the point where I can't bring myself to believe
# it's what the authors actually used).  I have filled in the gaps as
# made best sense to me, so this should be considered a new algorithm
# drawing inspiration from the above (and others).

from gumbo cimport *

from grapheme_counter cimport n_grapheme_clusters
from unicodedata import normalize as unicode_norm
from re import compile as regex

# All text chunks are fed through a normalizer before processing.
# We apply NFKC despite its occasionally destructive consequences,
# because downstream processing really needs some of the transforms it
# does (full/halfwidth CJK, removal of ligatures) and shouldn't be
# sensitive to things like losing mathematical semantics.  We also
# compress all runs of whitespace to a single U+0020, and strip
# leading and trailing spaces; this is even more aggressive than NFKC,
# which, for instance, converts U+2000 through U+200A into U+0020, but
# leaves U+0009 and U+000A alone, and doesn't collapse runs.
_whitespace = regex("\\s+")
cdef unicode _normalize1(unicode text):
    return _whitespace.sub(" ", unicode_norm('NFKC', text)).strip()

cdef unicode _normalizev(text):
    return normalize1("".join(text))

# Length of the canonicalized serialization of a tag attribute.
# Attribute names are (supposed to be) ASCII.
cdef size_t attr_len(GumboAttribute* attr):
    return (4 + # space, equals sign, two quote marks
            len(attr.name.decode('ascii')) +
            n_grapheme_clusters(_normalize1(attr.value.decode('utf-8'))))

cdef TagClass classify_tag(GumboTag tag):
    # Elements that do not display their children.
    # XXX Ideally we would be treating the contents of <iframe> as
    # inlined into the parent document, but it's too late for that now;
    # the crawler didn't record them.
    if tag in (GUMBO_TAG_APPLET,
               GUMBO_TAG_DATALIST,
               GUMBO_TAG_FRAME,
               GUMBO_TAG_FRAMESET,
               GUMBO_TAG_HEAD,
               GUMBO_TAG_IFRAME,
               GUMBO_TAG_NOFRAMES,
               GUMBO_TAG_NOSCRIPT,
               GUMBO_TAG_SCRIPT,
               GUMBO_TAG_STYLE,
               GUMBO_TAG_TEMPLATE):
        return TC_DISCARD

    # Hyperlinks are a special case: they are inline elements, but
    # their text is weighted less than other text (see below).
    if tag in (GUMBO_TAG_A,):
        return TC_LINK

    # "Inline" elements.  Not exactly display:inline; more like
    # "elements that can naturally appear in the middle of a
    # paragraph".  Has a pretty close, but not exact, correspondence
    # with the elements defined in HTML5 4.5 "Text-level semantics".
    # Form elements are intentionally excluded.
    if tag in (GUMBO_TAG_ABBR,
               GUMBO_TAG_ACRONYM,
               GUMBO_TAG_ADDRESS,
               GUMBO_TAG_B,
               GUMBO_TAG_BDI,
               GUMBO_TAG_BDO,
               GUMBO_TAG_BIG,
               GUMBO_TAG_BLINK,
               GUMBO_TAG_CITE,
               GUMBO_TAG_CODE,
               GUMBO_TAG_DATA,
               GUMBO_TAG_DEL,
               GUMBO_TAG_DFN,
               GUMBO_TAG_EM,
               GUMBO_TAG_FONT,
               GUMBO_TAG_I,
               GUMBO_TAG_INS,
               GUMBO_TAG_KBD,
               GUMBO_TAG_MARK,
               GUMBO_TAG_NOBR,
               GUMBO_TAG_Q,
               GUMBO_TAG_RB,
               GUMBO_TAG_RP,
               GUMBO_TAG_RT,
               GUMBO_TAG_RUBY,
               GUMBO_TAG_S,
               GUMBO_TAG_SAMP,
               GUMBO_TAG_SMALL,
               GUMBO_TAG_SPAN,
               GUMBO_TAG_STRIKE,
               GUMBO_TAG_STRONG,
               GUMBO_TAG_SUB,
               GUMBO_TAG_SUP,
               GUMBO_TAG_TIME,
               GUMBO_TAG_TT,
               GUMBO_TAG_U,
               GUMBO_TAG_VAR,
               GUMBO_TAG_WBR,

               # math
               GUMBO_TAG_MATH,
               GUMBO_TAG_MALIGNMARK,
               GUMBO_TAG_MGLYPH,
               GUMBO_TAG_MI,
               GUMBO_TAG_MN,
               GUMBO_TAG_MO,
               GUMBO_TAG_MS,
               GUMBO_TAG_MTEXT):
        return TC_INLINE

    # Elements that introduce graphical displays.  These are treated as
    # either TC_BLOCK or TC_INLINE depending on the surrounding context.
    # Treating <audio> and <bgsound> as "graphic" is a little weird
    # but it does the right thing wrt the heuristics below.
    # XXX We might conceivably want to drill down into an <svg> to find
    # <text>, <desc>, <title>, etc.
    if tag in (GUMBO_TAG_AUDIO,
               GUMBO_TAG_BGSOUND,
               GUMBO_TAG_CANVAS,
               GUMBO_TAG_EMBED,
               GUMBO_TAG_IMG,
               GUMBO_TAG_OBJECT,
               GUMBO_TAG_SVG,
               GUMBO_TAG_VIDEO):
        return TC_GRAPHIC

    # Elements that introduce "heading content" according to HTML5.
    # These are treated the same as TC_BLOCK except that they merge
    # with a subsequent run of paragraph content if present.
    #
    # Note that <header> does *not* count as "heading content".
    # (What we want here is the outline, not the page-header.)
    if tag in (GUMBO_TAG_H1,
               GUMBO_TAG_H2,
               GUMBO_TAG_H3,
               GUMBO_TAG_H4,
               GUMBO_TAG_H5,
               GUMBO_TAG_H6,
               GUMBO_TAG_HGROUP):
        return TC_HEADING

    # Elements that are used to structure text into paragraphs.  These
    # correspond vaguely to the concept of "basic text elements" in
    # paper #1. Their treatment differs from TC_BLOCK in two ways:
    # first, we look through nesting of these within each other;
    # second, we merge a contiguous sequence of them into a single
    # BlockTreeNode.  Note that <article> and <section> are
    # intentionally *not* included in this set.
    if tag in (GUMBO_TAG_P,
               GUMBO_TAG_PRE,
               GUMBO_TAG_BLOCKQUOTE,
               GUMBO_TAG_OL,
               GUMBO_TAG_UL,
               GUMBO_TAG_LI,
               GUMBO_TAG_DL,
               GUMBO_TAG_DT,
               GUMBO_TAG_DD,
               GUMBO_TAG_FIGURE,
               GUMBO_TAG_FIGCAPTION):
        return TC_PARA

    # Everything else is treated as a block.
    return TC_BLOCK

cdef class BlockTreeNode:
    """One element in a "block tree", which is constructed from the
       Gumbo node tree according to an approximation of the "DOM2Blocks"
       algorithm in paper #1.  Several aspects of this algorithm are
       described poorly, and have been replaced with my best guess as
       to what was actually meant.  In particular, BlockTreeNode plays
       the same role in this algorithm as a "BLE&IE" block from that
       paper, but may have a substantially different definition.

       Each BlockTreeNode corresponds to a subtree of the original DOM
       tree, and can be thought of as a _contiguous run of text_.

       BlockTreeNodes have the following exposed properties:

           children  - List of BlockTreeNodes (may be empty) constructed
                       from child subtrees of this node's subtree.

           text      - String (may be empty): Text considered to
                       belong directly to this BlockTreeNode.

           tagchars -  Number of characters devoted to defining tags
                       and tag attributes, within this BlockTreeNode
                       (but not its children).  TC_INLINE tags are not
                       counted.

           textchars - Number of characters devoted to text, within
                       this BlockTreeNode (but not its children).

           totaltagchars - Total number of characters devoted to defining
                           tags and tag attributes, within this BlockTreeNode
                           and all of its children.

           totaltextchars - Total number of characters devoted to text,
                            within this BlockTreeNode and all of its children.

       All of the 'chars' properties count Unicode (extended
       untailored) grapheme clusters, not raw codepoints.  Moreover,
       all text is normalized before counting happens.

    """
    # Internal state
    cdef list    _textv

    # Exposed state
    cdef readonly unicode text
    cdef readonly list children
    cdef readonly size_t tagchars, \
                         textchars, \
                         totaltagchars, \
                         totaltextchars

    def __cinit__(self):
        self.text = None
        self._textv = []
        self.children = []

    # Internal-use-only methods

    cdef bint add_text(self, unicode text) except False:
        """Count TEXT as part of the text content of this block."""
        if self._textv is None:
            raise RuntimeError("add_text called on finalized BlockTreeNode")
        self._textv.append(text)
        return True

    cdef bint add_tag(self, unicode tagname, GumboElement tag) except False:
        """Count TAG as part of the non-text content of this block.

           Note: this function is not responsible for deciding whether to
           split the block, and it does not inspect the children of the
           element.  All it does is update tagchars."""
        if self._textv is None:
            raise RuntimeError("add_tag called on finalized BlockTreeNode")

        # Tag names are known to be ASCII.
        self.tagchars += 2 # <>
        self.tagchars += len(tagname)

        for i in range(tag.attributes.length):
            self.tagchars += attr_len(<GumboAttribute*> tag.attributes.data[i])

    cdef bint add_child(self, BlockTreeNode child) except False:
        """CHILD is a BlockTreeNode; count it as one of the children of this
           block."""
        if self._textv is None:
            raise RuntimeError("add_child called on finalized BlockTreeNode")
        if child._textv is not None:
            raise RuntimeError("add_child given a non-finalized BlockTreeNode")
        self.children.append(text)
        return True

    cdef bint finalize(self) except False:
        self.text = _normalizev(self._textv)
        self._textv = None

        self.textchars = n_grapheme_clusters(self.text)
        self.totaltagchars = self.tagchars
        self.totaltextchars = self.textchars
        for c in self.children:
            self.totaltagchars += c.totaltagchars
            self.totaltextchars += c.totaltextchars
