"""Extract content from HTML pages.  This is a wrapper around the
Gumbo HTML5 parser library; for efficiency we need to do the tree
walking as well as the parsing in C(ython).
"""

from gumbo cimport *
from prescan cimport *
from mimesniff cimport *

from .unicode_utils cimport strip_ascii_space, split_ascii_space, normalize_text
from .boilerplate_removal cimport *
from .relative_urls cimport urljoin, urljoin_outbound

from re import compile as _Regexp
from re import DOTALL  as _Re_DOTALL
from collections import Counter as _Counter
from chardet import detect as detect_encoding_statistically

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

cdef inline bint _X_(GumboElement *element, bytes attr,
                     list links) except False:
    """Helper: If ATTR is present on ELEMENT, extract its value, strip
    ASCII whitespace and append the result to LINKS."""
    v = get_htmlattr(element, attr)
    if v is not None:
        links.append(strip_ascii_space(v))
    return True

cdef bytes convert_to_utf8(bytes page, str encoding):
    """Convert PAGE from ENCODING to UTF-8 and strip any byte order mark."""

    cdef str intermediate

    assert encoding != "replacement" and encoding != "x-user-defined"

    # Python knows these encodings only by other names.
    # https://bugs.python.org/issue25416
    if encoding == "windows-874":
        encoding = "cp874"
    elif encoding == "x-mac-cyrillic":
        encoding = "mac_cyrillic"
    # The Encoding Standard makes a distinction between iso-8859-8-i
    # (logical order Hebrew) and iso-8859-8(-e) (visual order Hebrew)
    # because HTML5 makes *some* attempt to handle visual-order Hebrew
    # (our parser doesn't, though).  At the codecs level they are
    # identical, and Python doesn't know the -i alias
    # (https://bugs.python.org/issue18624).
    elif encoding == "iso-8859-8-i":
        encoding = "iso-8859-8"

    if encoding != "utf-8":
        # This is a type hint.  Without the intermediate variable,
        # Cython doesn't realize it can use PyUnicode_AsUTF8String
        # for the second step.
        intermediate = page.decode(encoding)
        page = intermediate.encode("utf-8")
    if page.startswith(b"\xef\xbb\xbf"):
        page = page[3:]
    return page

cdef inline bytes determine_encoding_and_convert(bytes page,
                                                 bytes ext_encoding):

    """Determine the encoding of PAGE, following HTML5's "encoding
       sniffing algorithm"
       (https://html.spec.whatwg.org/multipage/syntax.html#determining-the-character-encoding).
       EXT_ENCODING may be either b"" or an encoding label in the
       list at https://encoding.spec.whatwg.org/#names-and-labels.

       Regardless of what the encoding was detected to be, the return value
       is PAGE converted to UTF-8, with BOM (if any) stripped.

    """
    cdef const char *rv
    cdef str encoding

    # Step 1 does nothing (there is no user override).
    # Step 2 does nothing (we already have the full text).

    # Step 3
    if page.startswith(b"\xfe\xff"):
        return convert_to_utf8(page, "utf-16be")
    if page.startswith(b"\xff\xfe"):
        return convert_to_utf8(page, "utf-16le")
    if page.startswith(b"\xef\xbb\xbf"):
        return page[3:] # already UTF-8, just strip the BOM

    # Step 4
    if ext_encoding:
        rv = canonical_encoding_for_label(ext_encoding)
        if rv:
            encoding = rv.decode("ascii")
            return convert_to_utf8(page, encoding)

    # Step 5
    rv = prescan_a_byte_stream_to_determine_its_encoding(
        page, min(len(page), 1024))
    if rv:
        encoding = rv.decode('ascii')
        # This is mandated by HTML5, with the justification that
        # if the page were _really_ in utf-16 then the prescanner
        # wouldn't have been able to parse it.
        if encoding.startswith("utf-16"):
            encoding = "utf-8"

        # HTML5 specifically says _not_ to let "replacement" and
        # "x-user-defined" through to steps 7-9.  This is because
        # HTML5 is more concerned with blocking XSS smuggling via
        # rarely-used, non-ASCII-superset encodings than with keeping
        # old pages working.  Our concerns are precisely the opposite.
        if encoding != "replacement" and encoding != "x-user-defined":
            return convert_to_utf8(page, encoding)

    # Step 6 does nothing (there is no parent browsing context)

    # Steps 7, 8, and 9
    # senc might be None, so not reusing 'encoding' for it
    senc = detect_encoding_statistically(page).get('encoding', '')
    # HTML5's last-ditch default
    if not senc: senc = 'windows-1252'

    return convert_to_utf8(page, senc)

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

        elif node.type in (GUMBO_NODE_COMMENT,
                           GUMBO_NODE_TEMPLATE):
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
    original     - Bytes: the original HTML of the page, converted to UTF-8
                   if necessary.
    mimetype     - The computed MIME type of the page.
    """

    cdef readonly unicode url, title, mimetype, text_content
    cdef readonly unicode text_pruned
    cdef readonly object blocktree # for debugging
    cdef readonly double threshold # ditto
    cdef readonly object links, resources, headings, dom_stats
    cdef readonly bytes original

    def __init__(self, url, page, external_ctype='text/html',
                 external_charset='utf-8'):

        cdef size_t pagelen
        cdef char *pagebuf
        cdef GumboOptions opts
        cdef GumboOutput *output

        self.url = url
        self.dom_stats = DomStatistics()

        mimetype = external_ctype.casefold().encode("ascii")
        charset  = external_charset.casefold().encode("ascii")

        if isinstance(page, str):
            # Without the cast, Cython doesn't realize it can use
            # PyUnicode_AsUTF8String here.
            bytestr = (<str>page).encode('utf-8')
            mimetype = get_computed_mimetype(mimetype, charset,
                                             bytestr, len(bytestr))

        else:
            mimetype = get_computed_mimetype(mimetype, charset,
                                             page, len(page))
            bytestr = determine_encoding_and_convert(page, charset)

        mimetype = mimetype.decode("ascii")
        if mimetype != "text/html":
            # Match what PhantomJS does when asked to load certain types of
            # non-HTML resources as the base document.
            if mimetype.startswith("text/"):
                bytestr = (b"<html><head></head><body><pre style=\"word-wrap: "
                           b"break-word; white-space: pre-wrap;\">" +
                           bytestr.replace(b"&", b"&amp;")
                                  .replace(b"<", b"&lt;")
                                  .replace(b">", b"&gt;") +
                           b"</pre></body></html>")

            elif mimetype.startswith("image/"):
                bytestr = ("<html><body style=\"margin: 0px;\">"
                           "<img style=\"-webkit-user-select: none\" src=\"" +
                           url.replace(b"&", b"&amp;")
                              .replace(b"<", b"&lt;")
                              .replace(b">", b"&gt;") +
                           "\"></body></html>").encode("utf-8")

            else:
                bytestr = b"<html><head></head><body></body></html>"

        self.mimetype = mimetype
        self.original = bytestr
        pagebuf = bytestr
        pagelen = len(bytestr)

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
        self.blocktree    = walker.builder.tree

        tp, thresh        = extract_content(self.blocktree)
        self.text_pruned  = tp
        self.threshold    = thresh
