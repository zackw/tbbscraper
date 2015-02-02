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
#
# The block-merging heuristics, and the concept of just counting
# _characters_ (grapheme clusters) in both tags and text, are more
# from paper #1; the "total text density" metric and the self-tuning
# threshold are more from paper #2.

cimport gumbo
from gumbo cimport *
from .unicode_utils cimport n_grapheme_clusters, \
    normalize_text, not_all_whitespace

__all__ = [
    "classify_tag",
    "BlockTreeNode",
    "BlockTreeBuilder",
    "extract_content"
]

TAGCLASS_LABELS = [
    "discard",
    "inline",
    "link",
    "block",
    "heading",
    "para",
    "graphic",
    "root"
]

# Length of the canonicalized serialization of a tag attribute.
# Attribute names are (supposed to be) ASCII.
cdef inline Py_ssize_t attr_len(GumboAttribute* attr) except -1:
    return (4 + # space, equals sign, two quote marks
            len(attr.name.decode('ascii')) +
            n_grapheme_clusters(normalize_text(attr.value.decode('utf-8'))))

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
    # Form elements are intentionally excluded.  Note that <wbr> is an
    # *optional* break (like &shy; but displays nothing if that break
    # is selected).
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

    # Elements that introduce graphical displays.
    # Treating <audio> and <bgsound> as "graphic" is a little weird
    # but it does the right thing wrt the heuristics below.
    # XXX We might conceivably want to drill down into an <svg> to find
    # <text>, <desc>, <title>, etc.  And we are currently discarding
    # <canvas> fallback content.
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

    if tag in (GUMBO_TAG_HTML,):
        return TC_ROOT

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

           tagclass  - TagClass: classification of this block.

           tagchars  - Number of characters devoted to defining tags
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

    def __cinit__(self, TagClass tclass, int depth):
        self.tagclass = tclass
        self.text = None
        self._textv = []
        self.children = []
        assert depth >= 0
        self._depth = depth

    cdef bint _dump_tree(self, outf, textwrap,
                         int depth, double thresh) except False:
        indent = "  "*depth

        if self.totaltextdensity >= thresh:
            outf.write("\033[7m")

        outf.write("{}{}: local {:.3f} ({}/{}) total {:.3f} ({}/{})\n"
                   .format(indent, TAGCLASS_LABELS[self.tagclass],
                           self.textdensity,
                           self.textchars, self.tagchars,
                           self.totaltextdensity,
                           self.totaltextchars, self.totaltagchars))
        if self.text:
            indent = "  "*(depth+1)
            outf.write(textwrap.fill(self.text,
                                     width=70 + 2*(depth+1),
                                     initial_indent=indent,
                                     subsequent_indent=indent))
            outf.write("\n")

        if self.totaltextdensity > thresh:
            outf.write("\033[27m")

        for c in self.children:
            outf.write("\n")
            (<BlockTreeNode>c)._dump_tree(outf, textwrap, depth+1, thresh)

        return True

    def dump_tree(self, outf, thresh=None):
        import textwrap
        if thresh is None:
            thresh = float("+inf")
        self._dump_tree(outf, textwrap, 0, thresh)

    # Internal-use-only methods

    cdef bint add_text(self, unicode text) except False:
        """Count TEXT as part of the text content of this block."""
        if self._textv is None:
            raise RuntimeError("add_text called on finalized BlockTreeNode")
        self._textv.append(text)
        return True

    cdef bint add_tag(self, unicode tagname, GumboElement* tag) except False:
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

        return True

    cdef bint add_child(self, BlockTreeNode child) except False:
        """CHILD is a BlockTreeNode; count it as one of the children of this
           block."""
        if self._textv is None:
            raise RuntimeError("add_child called on finalized BlockTreeNode")
        self.children.append(child)
        return True

    cdef bint finalize(self) except False:
        if self._textv is None:
            return True

        self.text   = normalize_text(self._textv)
        self._textv = None
        self._depth = -1

        self.textchars = n_grapheme_clusters(self.text)
        self.totaltagchars = self.tagchars
        self.totaltextchars = self.textchars
        for cc in self.children:
            c = <BlockTreeNode>cc
            c.finalize()
            self.totaltagchars += c.totaltagchars
            self.totaltextchars += c.totaltextchars

        if self.tagchars == 0:
            self.textdensity = self.textchars
        else:
            self.textdensity = self.textchars / <double>self.tagchars

        if self.totaltagchars == 0:
            self.totaltextdensity = self.totaltextchars
        else:
            self.totaltextdensity = self.totaltextchars / \
                                    <double>self.totaltagchars

        return True

cdef class BlockTreeBuilder:
    """Class which holds all the state involved in constructing a block
    tree.  There's one instance of this object per walker_state (see
    _extractor.pyx) and it's invoked for every GumboNode."""

    def __cinit__(self):
        self.depth      = 0
        self.in_discard = 0
        self.stack      = []
        self.tree       = None

    cdef bint maybe_resume_prev_or_parent(self,
                                          unsigned int depth,
                                          bint is_heading) except False:
        """We have just encountered a tag whose block might be mergeable
           with either its immediate previous sibling at this level, or
           with its direct parent.  Decide whether or not to do this, and
           either way, adjust the stack appropriately.

           All such blocks are considered to be either TC_HEADING or
           TC_PARA blocks (although they might not actually have been
           induced by a tag in one of those classes)."""

        cdef TagClass pclass
        cdef TagClass tclass = TC_PARA
        if is_heading:
            tclass = TC_HEADING

        # We obviously cannot merge with anything if the stack is
        # empty.  This should never happen -- there should always be a
        # topmost block, for <body> -- but just to be safe.
        if not self.stack:
            self.stack.append(BlockTreeNode(tclass, depth))
            return True

        # If this block is the first child at this level, it might be
        # mergeable with its parent.
        if not (<BlockTreeNode>self.stack[-1]).children:
            pclass = (<BlockTreeNode>self.stack[-1]).tagclass
            if tclass == pclass:
                # Can merge, stack is already right for that.
                # Do not override the depth; we want this block to
                # stay on the stack until we exit the parent.
                return True

            elif tclass == TC_PARA and pclass == TC_HEADING:
                # Can merge, change parent to TC_PARA.
                (<BlockTreeNode>self.stack[-1]).tagclass = TC_PARA
                return True

        # Otherwise, this block might be mergeable with its immediate
        # previous sibling.
        else:
            pclass = (<BlockTreeNode>self.stack[-1]).children[-1].tagclass
            if (tclass == pclass or
                (tclass == TC_PARA and pclass == TC_HEADING)):

                self.stack.append((<BlockTreeNode>self.stack[-1])
                                  .children.pop())
                (<BlockTreeNode>self.stack[-1]).tagclass = TC_PARA
                (<BlockTreeNode>self.stack[-1])._depth = <int>depth
                return True

        # Otherwise, we cannot merge.
        self.stack.append(BlockTreeNode(tclass, depth))
        return True


    cdef bint enter_elt(self, TagClass tclass, unicode tname,
                        GumboElement* tag) except False:

        assert self.tree is None

        self.depth += 1
        assert self.depth >= 0
        if self.in_discard:
            if tclass == TC_DISCARD:
                self.in_discard += 1
            return True

        # Discarded elements are completely ignored.
        if tclass == TC_DISCARD:
            self.in_discard += 1
            assert self.in_discard >= 0
            return True

        # Inline tags do not count toward the tag weight of the block.
        # They act as TC_PARA tags for merge purposes.
        elif tclass == TC_INLINE:
            self.maybe_resume_prev_or_parent(self.depth, False)
            return True

        # Block tags always induce a new block.
        elif tclass == TC_BLOCK or tclass == TC_ROOT:
            self.stack.append(BlockTreeNode(tclass, self.depth))

        # Heading tags merge with a previous or parent heading block.
        elif tclass == TC_HEADING:
            self.maybe_resume_prev_or_parent(self.depth, True)

        # Paragraph and link tags merge with a previous or parent
        # which is either TC_PARA or TC_HEADING; in the latter case
        # they convert it to TC_PARA.
        elif tclass in (TC_PARA, TC_LINK):
            self.maybe_resume_prev_or_parent(self.depth, False)

        # Graphic elements never induce blocks, and their contents
        # are discarded, but their own attributes count toward the
        # tag weight of the parent block, whatever it is.
        elif tclass == TC_GRAPHIC:
            self.in_discard += 1

        else:
            assert not "Missing TagClass case?"

        (<BlockTreeNode>self.stack[-1]).add_tag(tname, tag)
        return True

    cdef bint exit_elt(self, TagClass tclass) except False:

        assert self.tree is None
        assert self.stack

        self.depth -= 1
        if tclass in (TC_DISCARD, TC_GRAPHIC):
            self.in_discard -= 1

        while self.depth < (<BlockTreeNode>self.stack[-1])._depth:
            block = <BlockTreeNode> self.stack.pop()

            if not self.stack:
                # We have exited the topmost DOM node and we are now done.
                block.finalize()
                self.tree = block
                break

            else:
                # We don't finalize this block immediately, because
                # subsequent blocks might merge with it.
                (<BlockTreeNode>self.stack[-1]).add_child(block)

        return True

    cdef bint add_text(self, unicode text) except False:
        # Only add text nodes if we're not discarding, obviously.
        # Additional special case: if the parent is TC_BLOCK or TC_ROOT
        # and the text is _entirely_ whitespace, throw it away.
        # (This avoids creation of junk paragraphs just because the HTML
        # is neatly indented.)
        if (not self.in_discard and
            ((<BlockTreeNode>self.stack[-1]).tagclass
             not in (TC_BLOCK, TC_ROOT) or
             not_all_whitespace(text))):

            # A bare text node counts as TC_PARA for merge purposes.
            # It also counts as being one deeper than the current nest.
            self.maybe_resume_prev_or_parent(self.depth+1, False)
            (<BlockTreeNode>self.stack[-1]).add_text(text)

        return True

#
# This part of the algorithm is responsible for figuring out what
# is and isn't content, and extracting the right bits.
#

cdef BlockTreeNode find_max_density(BlockTreeNode node,
                                    BlockTreeNode candidate):
    if candidate is None or node.totaltextdensity > candidate.totaltextdensity:
        candidate = node

    for cc in node.children:
        c = <BlockTreeNode>cc
        candidate = find_max_density(c, candidate)

    return candidate

cdef list find_path_to_max_density(BlockTreeNode node,
                                   BlockTreeNode target):
    if node is target:
        return [node]

    for cc in node.children:
        c = <BlockTreeNode>cc
        p = find_path_to_max_density(c, target)
        if p is not None:
            return [node] + p

    return None

cdef double choose_threshold(BlockTreeNode root) except -1:
    # paper 2, paraphrased: "...first find the _maximum_ density
    # block in the whole page; then, take the _minimum_ density
    # in the path from that block to the body as the threshold."

    assert root.tagclass == TC_ROOT
    assert len(root.children) == 1

    body = <BlockTreeNode> root.children[0]
    target = find_max_density(body, None)
    path   = find_path_to_max_density(body, target)

    return min((<BlockTreeNode>x).totaltextdensity for x in path)

#
# Paper 2's description of how content is actually selected is very
# confusing and possibly internally inconsistent.  So we do the simple
# thing, which is to select all blocks whose totaltextdensity is above
# the threshold, whether or not their parents are above the threshold.
# This is not perfect; in particular it can lose real content when
# there are a lot of <div>s in the middle of the content area.  But
# it's good enough for our purposes.
#

cdef bint do_extract_content(BlockTreeNode node,
                             double thresh,
                             list output) except False:
    if node.totaltextdensity > thresh:
        output.append(node.text)

    for c in node.children:
        do_extract_content(<BlockTreeNode>c, thresh, output)

    return True

cpdef unicode extract_content(BlockTreeNode root):
    cdef broot = <BlockTreeNode>root
    cdef double thresh = choose_threshold(broot)

    selected_blocks = []
    do_extract_content(broot, thresh, selected_blocks)
    return " ".join(selected_blocks)
