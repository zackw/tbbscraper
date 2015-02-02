"""Extract content from HTML pages.  This is a wrapper around the
Gumbo HTML5 parser library; for efficiency we need to do the tree
walking as well as the parsing in C(ython).
"""

from gumbo cimport *
from .unicode_utils cimport strip_ascii_space, split_ascii_space, normalize_text
from .boilerplate_removal cimport *
from .relative_urls cimport urljoin, urljoin_outbound

from re import compile as _Regexp
from re import DOTALL  as _Re_DOTALL
from collections import Counter as _Counter

# "Common" tag names are those that have GUMBO_TAG_* constants.
_common_tagnames = [gumbo_normalized_tagname(<GumboTag>i).decode("utf-8")
                    for i in range(GUMBO_TAG_LAST)]

cdef inline list prune_outbound_urls(unicode doc, list urls):
    """Given a list of possibly-relative URLs, make them all absolute
    (with reference to DOC), then remove all duplicates and all links to
    or within DOC itself."""
    pruned = set()
    for url in urls:
        adjusted = urljoin_outbound(doc, url)
        if adjusted is not None:
            pruned.add(adjusted)
    return sorted(pruned)

cdef inline unicode get_htmlattr(GumboElement *element, bytes name):
    """Extract an attribute value from an HTML element."""
    cdef GumboAttribute* attr = gumbo_get_attribute(&element.attributes, name)
    if attr is NULL:
        return None
    return attr.value.decode("utf-8")

cdef inline bint _X_(GumboElement *element, bytes attr,
                     list links) except False:
    """Helper: If ATTR is present on ELEMENT, extract its value, strip
    ASCII whitespace and append the result to LINKS."""
    v = get_htmlattr(element, attr)
    if v is not None:
        links.append(strip_ascii_space(v))
    return True

# Main tree walker.  Since this gets compiled now, we are safe to just
# go ahead and use recursive function calls.

cdef class TreeWalker:
    cdef unsigned int depth,      \
                      in_discard, \
                      in_title,   \
                      in_head,    \
                      in_heading, \
                      saw_base_href
    cdef unicode url
    cdef object title,            \
                headings,         \
                text_content,     \
                tags,             \
                tags_at_depth,    \
                links,            \
                resources
    cdef BlockTreeBuilder builder

    def __cinit__(self, url, stats):
        self.depth         = 0
        self.in_discard    = 0
        self.in_title      = 0
        self.in_head       = 0
        self.in_heading    = 0
        self.saw_base_href = 0
        self.url           = url
        self.title         = []
        self.headings      = []
        self.text_content  = []
        self.tags          = stats.tags
        self.tags_at_depth = stats.tags_at_depth
        self.links         = []
        self.resources     = []
        self.builder       = BlockTreeBuilder()

    cdef bint finalize(self) except False:
        self.title        = normalize_text(self.title)
        self.text_content = normalize_text(self.text_content)
        self.headings     = [normalize_text(head) for head in self.headings]
        self.links        = prune_outbound_urls(self.url, self.links)
        self.resources    = prune_outbound_urls(self.url, self.resources)
        return True

    cdef inline bint extract_links(self, GumboElement *element) except False:
        cdef GumboTag tag = element.tag

        # resources
        if tag in (GUMBO_TAG_AUDIO,
                   GUMBO_TAG_EMBED,
                   GUMBO_TAG_IFRAME,
                   GUMBO_TAG_INPUT,
                   GUMBO_TAG_SCRIPT,
                   GUMBO_TAG_SOURCE,
                   GUMBO_TAG_TRACK):
            _X_(element, b"src", self.resources)

        elif tag == GUMBO_TAG_VIDEO:
            _X_(element, b"src", self.resources)
            _X_(element, b"poster", self.resources)

        elif tag == GUMBO_TAG_IMG:
            _X_(element, b"src", self.resources)
            # srcset is a comma-separated list of "image candidate strings",
            # each consisting of a URL possibly followed by spaces and then
            # "width" or "density" descriptors.  Note that leading spaces in
            # each field of the comma-separated list are to be discarded,
            # i.e. in srcset=" 1x", "1x" is the URL, not a descriptor.
            v = get_htmlattr(element, b"srcset")
            if v is not None:
                for ic in v.split(u","):
                    ic = split_ascii_space(ic)
                    if ic:
                        self.resources.append(ic[0])

        elif tag == GUMBO_TAG_OBJECT:
            _X_(element, b"data", self.resources)

        elif tag == GUMBO_TAG_MENUITEM:
            _X_(element, b"icon", self.resources)

        # hyperlinks
        elif tag in (GUMBO_TAG_A, GUMBO_TAG_AREA):
            _X_(element, b"href", self.links)

        elif tag == GUMBO_TAG_FORM:
            _X_(element, b"action", self.resources)

        elif tag in (GUMBO_TAG_BUTTON, GUMBO_TAG_INPUT):
            _X_(element, b"formaction", self.links)

        elif tag in (GUMBO_TAG_BLOCKQUOTE, GUMBO_TAG_DEL,
                     GUMBO_TAG_INS, GUMBO_TAG_Q):
            _X_(element, b"cite", self.links)


        # link[href] may be either a resource, an outbound link, or to-ignore
        # depending on the value of the rel= property.
        elif tag == GUMBO_TAG_LINK:
            href = get_htmlattr(element, b"href")
            rel  = get_htmlattr(element, b"rel")
            if href and rel:
                reltags = split_ascii_space(rel)
                for ty in ("icon", "pingback", "prefetch", "stylesheet"):
                    if ty in reltags:
                        self.resources.append(strip_ascii_space(href))
                        break
                else:
                    for ty in ("alternate", "author", "help", "license",
                               "next", "prev", "search", "sidebar"):
                        if ty in reltags:
                            self.links.append(strip_ascii_space(href))
                            break
        else:
            pass

        return True

    cdef bint update_dom_stats(self, unicode tagname) except False:
        self.tags_at_depth[self.depth] += 1
        self.tags[tagname] += 1
        return True

    cdef bint walk_text(self, const char *text) except False:
        if self.in_discard and not self.in_title:
            return True

        decoded = text.decode('utf-8')
        if self.in_title:
            self.title.append(decoded)

        if not self.in_discard:
            self.text_content.append(decoded)
            if self.in_heading:
                self.headings[-1].append(decoded)

        self.builder.add_text(decoded)

        return True

    cdef bint walk_element(self,
                           GumboElement *elt,
                           GumboParseFlags flags) except False:

        cdef unsigned int i
        cdef unicode tagname
        cdef const char *svg_tagname
        cdef bint elt_forces_word_break,  \
                  elt_is_title,           \
                  elt_is_heading,         \
                  elt_discards_contents

        if elt.tag != GUMBO_TAG_UNKNOWN:
            tagname = _common_tagnames[elt.tag]
        else:
            svg_tagname = NULL
            if elt.tag_namespace == GUMBO_NAMESPACE_SVG:
                svg_tagname = gumbo_normalize_svg_tagname(&elt.original_tag)

            if svg_tagname is not NULL:
                tagname = svg_tagname.decode("utf-8")
            else:
                tagname = (elt.original_tag.data[:elt.original_tag.length]
                           .decode("utf-8"))

        # Record only elements that appeared explicitly in the HTML
        # (whether or not they appeared exactly in the current DOM position).
        if flags in (GUMBO_INSERTION_NORMAL,
                     GUMBO_INSERTION_IMPLICIT_END_TAG,
                     GUMBO_INSERTION_CONVERTED_FROM_END_TAG,
                     GUMBO_INSERTION_ADOPTION_AGENCY_MOVED,
                     GUMBO_INSERTION_FOSTER_PARENTED):
            self.update_dom_stats(tagname)

        # Very special case for /html/head/base the first time it's seen
        # (HTML5 requires ignoring misplaced <base>, and second and
        # subsequent instances of <base>)
        if (elt.tag == GUMBO_TAG_BASE
            and not self.saw_base_href
            and self.depth == 2
            and self.in_head == 1):
            self.saw_base_href = 1
            href = get_htmlattr(elt, b"href")
            if href:
                href = strip_ascii_space(href)
            if href:
                self.url = urljoin(self.url, href)

        self.extract_links(elt)

        tclass = classify_tag(elt.tag)
        elt_is_head           = elt.tag == GUMBO_TAG_HEAD
        elt_is_title          = elt.tag == GUMBO_TAG_TITLE
        elt_is_heading        = tclass  == TC_HEADING
        elt_discards_contents = tclass  == TC_DISCARD
        elt_forces_word_break = forces_word_break_p(tclass)

        self.builder.enter_elt(tclass, tagname, elt)
        if elt_forces_word_break:
            self.walk_text(" ")

        if elt.children.length == 0:
            self.builder.exit_elt(tclass)
            return True # empty element, we're done

        self.depth += 1
        if elt_discards_contents:
            self.in_discard += 1
        if elt_is_title:
            self.in_title += 1
        if elt_is_head:
            self.in_head += 1
        if elt_is_heading:
            self.in_heading += 1
            if self.in_heading == 1:
                self.headings.append([])

        try:
            for i in range(elt.children.length):
                self.walk_node(<GumboNode*>elt.children.data[i])
        finally:
            self.depth -= 1
            if elt_discards_contents:
                self.in_discard -= 1
            if elt_is_title:
                self.in_title -= 1
            if elt_is_head:
                self.in_head -= 1
            if elt_is_heading:
                self.in_heading -= 1

        if elt_forces_word_break:
            self.walk_text(" ")
        self.builder.exit_elt(tclass)

        return True

    cdef bint walk_node(self, GumboNode *node) except False:
        if node.type == GUMBO_NODE_ELEMENT:
            self.walk_element(&node.v.element, node.parse_flags)

        elif node.type in (GUMBO_NODE_TEXT,
                           GUMBO_NODE_CDATA,
                           GUMBO_NODE_WHITESPACE):
            self.walk_text(node.v.text.text)

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
    text_pruned  - String: text content with all the boilerplate removed.
    links        - Array of strings: all outbound links from this page.
                   Relative URLs have already been made absolute.
    resources    - Array of strings: all resources referenced by this page.
                   Relative URLs have already been made absolute.
    dom_stats    - DomStatistics object calculated from this page.
    """

    cdef public unicode url, title, text_content
    cdef public object text_pruned
    cdef public object links, resources, headings, dom_stats

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
            walker = TreeWalker(self.url, self.dom_stats)
            walker.walk_node(output.root)
            walker.finalize()
        finally:
            gumbo_destroy_output(&opts, output)

        self.url          = walker.url
        self.title        = walker.title
        self.text_content = walker.text_content
        self.headings     = walker.headings
        self.links        = walker.links
        self.resources    = walker.resources
        self.text_pruned  = extract_content(walker.builder.tree)
