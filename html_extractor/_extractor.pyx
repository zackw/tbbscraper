"""Extract content from HTML pages.  This is a wrapper around the
Gumbo HTML5 parser library; for efficiency we need to do the tree
walking as well as the parsing in C(ython).
"""

from gumbo cimport *
from re import compile as _Regexp
from collections import Counter as _Counter

# "Common" tag names are those that have GUMBO_TAG_* constants.
_common_tagnames = [gumbo_normalized_tagname(<GumboTag>i).decode("utf-8")
                    for i in range(GUMBO_TAG_LAST)]

_WSRE = _Regexp(u"\\s+")
cdef unicode merge_text(textvec):
    """Merge a list of strings, then compact every run of White_Space
    characters within that list to a single space character.
    """
    merged = u"".join(textvec)
    return _WSRE.sub(u" ", merged).strip()

cdef inline bint is_heading(GumboTag tag):
    """Elements that introduce "heading content" according to HTML5.
    Note that <header> does *not* count as "heading content".
    (What we want here is the outline, not the page-header.)
    """
    return tag in (GUMBO_TAG_H1,
                   GUMBO_TAG_H2,
                   GUMBO_TAG_H3,
                   GUMBO_TAG_H4,
                   GUMBO_TAG_H5,
                   GUMBO_TAG_H6,
                   GUMBO_TAG_HGROUP)

cdef inline bint discards_contents(GumboTag tag):
    """Elements that do not display their children.  <canvas> is excluded
    from this list because we want to capture its fallback content, if any.
    """
    return tag in (GUMBO_TAG_AUDIO,
                   GUMBO_TAG_EMBED,
                   GUMBO_TAG_HEAD,
                   GUMBO_TAG_IFRAME,
                   GUMBO_TAG_IMG,
                   GUMBO_TAG_NOFRAMES,
                   GUMBO_TAG_NOSCRIPT,
                   GUMBO_TAG_OBJECT,
                   GUMBO_TAG_SCRIPT,
                   GUMBO_TAG_STYLE,
                   GUMBO_TAG_TEMPLATE,
                   GUMBO_TAG_VIDEO)

cdef inline bint forces_word_break(GumboTag tag):
   """Elements whose presence forces a word break.  For instance,
   "con<i>sis</i>tent" should produce "consistent", but
   "con<p>sis</p>tent" should produce "con sis tent".

   The list is of the elements whose presence should _not_ force
   a word break, because that list is shorter. """
   return tag not in (GUMBO_TAG_A,
                      GUMBO_TAG_ABBR,
                      GUMBO_TAG_B,
                      GUMBO_TAG_BASEFONT,
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
                      GUMBO_TAG_MALIGNMARK,
                      GUMBO_TAG_MARK,
                      GUMBO_TAG_MGLYPH,
                      GUMBO_TAG_MI,
                      GUMBO_TAG_MN,
                      GUMBO_TAG_MO,
                      GUMBO_TAG_MS,
                      GUMBO_TAG_MTEXT,
                      GUMBO_TAG_NOBR,
                      GUMBO_TAG_PLAINTEXT,
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
                      GUMBO_TAG_VAR)

# Main tree walker.  Since this gets compiled now, we are safe to just
# go ahead and use recursive function calls.

cdef class walker_state:
    cdef unsigned int depth,      \
                      in_discard, \
                      in_title,   \
                      in_heading
    cdef object title,            \
                headings,         \
                text_content,     \
                tags,             \
                tags_at_depth

    def __init__(self, stats):
        self.title = []
        self.headings = []
        self.text_content = []
        self.tags = stats.tags
        self.tags_at_depth = stats.tags_at_depth

cdef bint walk_text(const char *text, walker_state state) except False:
    if state.in_discard and not state.in_title:
        return True

    decoded = text.decode('utf-8')
    if state.in_title:
        state.title.append(decoded)

    if not state.in_discard:
        state.text_content.append(decoded)
        if state.in_heading:
            state.headings[-1].append(decoded)

    return True

cdef bint update_dom_stats(unicode tagname, walker_state state) except False:
    state.tags_at_depth[state.depth] += 1
    state.tags[tagname] += 1
    return True

#forward-declare
cdef extern bint walk_node(GumboNode*, walker_state) except False

cdef bint walk_element(GumboElement *elt, GumboParseFlags flags,
                       walker_state state) except False:

    cdef unsigned int i
    cdef unicode tagname
    cdef const char *svg_tagname
    cdef bint elt_forces_word_break,  \
              elt_is_title,           \
              elt_is_heading,         \
              elt_discards_contents

    # Record only elements that appeared explicitly in the HTML
    # (whether or not they appeared exactly in the current DOM position).
    if flags in (GUMBO_INSERTION_NORMAL,
                 GUMBO_INSERTION_IMPLICIT_END_TAG,
                 GUMBO_INSERTION_CONVERTED_FROM_END_TAG,
                 GUMBO_INSERTION_ADOPTION_AGENCY_MOVED,
                 GUMBO_INSERTION_FOSTER_PARENTED):

        if elt.tag != GUMBO_TAG_UNKNOWN:
            update_dom_stats(_common_tagnames[elt.tag], state)

        else:
            if elt.tag_namespace == GUMBO_NAMESPACE_SVG:
                svg_tagname = gumbo_normalize_svg_tagname(&elt.original_tag)
                if svg_tagname:
                    tagname = svg_tagname.decode("utf-8")
                else:
                    tagname = (elt.original_tag.data[:elt.original_tag.length]
                               .decode("utf-8"))
            else:
                tagname = (elt.original_tag.data[:elt.original_tag.length]
                           .decode("utf-8"))

            update_dom_stats(tagname, state)

    # Empty elements may still force word breaks.
    elt_forces_word_break = forces_word_break(elt.tag)
    if elt_forces_word_break:
        walk_text(" ", state)

    if elt.children.length == 0:
        return True # empty element, we're done

    elt_is_title          = elt.tag == GUMBO_TAG_TITLE
    elt_is_heading        = is_heading(elt.tag)
    elt_discards_contents = discards_contents(elt.tag)

    state.depth += 1
    if elt_discards_contents:
        state.in_discard += 1
    if elt_is_title:
        state.in_title += 1
    if elt_is_heading:
        state.in_heading += 1
        if state.in_heading == 1:
            state.headings.append([])

    try:
        for i in range(elt.children.length):
            walk_node(<GumboNode*>elt.children.data[i], state)
    finally:
        state.depth -= 1
        if elt_discards_contents:
            state.in_discard -= 1
        if elt_is_title:
            state.in_title -= 1
        if elt_is_heading:
            state.in_heading -= 1

    if elt_forces_word_break:
        walk_text(" ", state)

    return True

cdef bint walk_node(GumboNode *node, walker_state state) except False:
    if node.type == GUMBO_NODE_ELEMENT:
        walk_element(&node.v.element, node.parse_flags, state)

    elif node.type in (GUMBO_NODE_TEXT,
                       GUMBO_NODE_CDATA,
                       GUMBO_NODE_WHITESPACE):
        walk_text(node.v.text.text, state)

    elif node.type == GUMBO_NODE_COMMENT:
        pass

    else:
        # GUMBO_NODE_DOCUMENT should never occur.
        raise SystemError("unable to process node of type %u" % node.type)

    return True

#
# External API
#
cdef class DomStatistics:
    """Statistics about the DOM structure.  Has two attributes:

    tags - Dictionary of counters.  Each key is an HTML tag that
           appeared at least once in the document, with its spelling
           normalized.  The corresponding value is the number of times
           that tag appeared. Implicit tags are not counted.

    tags_at_depth - Dictionary of counters. Each key is a tree depth
                    in the document, and the corresponding value is
                    the number of times a tag appeared at that depth.
                    Depths containing only implicit tags are not counted.

    You can convert this object to a dictionary suitable for json.dump()
    by calling its to_json() method.
    """
    cdef public object tags
    cdef public object tags_at_depth

    def __init__(self):
        self.tags = _Counter()
        self.tags_at_depth = _Counter()

    def to_json(self):
        return { "tags"          : self.tags,
                 "tags_at_depth" : self.tags_at_depth }

cdef class ExtractedContent:
    """Content extracted from an HTML document.  Has the following fields:

    url          - String: URL of the page.  Will reflect <base href> if present.
    title        - String: Title of the page, i.e. the contents of the <title>
                   element.
    headings     - Array of strings: text of all the headings on the page, one
                   string per outermost <hN> or <hgroup> element.
    text_content - String: all visible text content on the page, including
                   the headings, but not the title.
    dom_stats    - DomStatistics object calculated from this page.
    """

    cdef public unicode url, title, text_content
    cdef public object headings, dom_stats

    def __init__(self, url, page):

        cdef size_t pagelen
        cdef char *pagebuf
        cdef GumboOptions opts
        cdef GumboOutput *output

        self.url = url
        self.dom_stats = DomStatistics()

        if isinstance(page, unicode):
            bytestr = page.encode('utf-8')
            pagebuf = bytestr
            pagelen = len(bytestr)
        else:
            pagebuf = page
            pagelen = len(page)

        opts = kGumboDefaultOptions
        opts.stop_on_first_error = False
        opts.max_errors = 0

        output = gumbo_parse_with_options(&opts, pagebuf, pagelen)
        if not output:
            raise RuntimeError("gumbo_parse returned nothing")

        try:
            state = walker_state(self.dom_stats)
            walk_node(output.root, state)
        finally:
            gumbo_destroy_output(&opts, output)

        self.title = merge_text(state.title)
        self.text_content = merge_text(state.text_content)
        self.headings = [merge_text(head) for head in state.headings]
