#! /usr/bin/python3

# Extract content from HTML pages.

import collections
import urllib.parse
import re
import sys
from gumbo import gumboc

# The HTML spec makes a distinction between "space characters" and
# "White_Space characters".  Only "space characters" are stripped
# from URL attributes.
_SRE  = re.compile("[ \t\r\n\f]+")
_WSRE = re.compile("\s+")

_SSRE = re.compile("^[ \t\r\n\f]+(.*?)[ \t\r\n\f]+$", re.DOTALL)
def _Sstrip(s):
    return _SSRE.sub("\\1", s)

# True if this is a link to an anchor within the current document.
def _within_this_document(docurl, url):
    return (urllib.parse.urldefrag(docurl).url ==
            urllib.parse.urldefrag(url).url)

# For each element that may have attribute(s) that are "hyperlink" or
# "resource" URLs, define how to extract them.
# All of these functions take the attribute dictionary for the element
# and return a 2-tuple of (key, [list of urls]) where key is "h" for
# hyperlinks or "r" for resources.

def _get_htmlattr(attrs, name):
    for attr in attrs:
        if attr.name == name:
            return attr.value.decode("utf-8")
    return None

def _X_(mode, attrs, a):
    urls = []
    for at in attrs:
        v = _get_htmlattr(a, at)
        if v is not None: urls.append(_Sstrip(v))
    return (mode, urls)

def _X_src(a):        return _X_("r", [b"src"],            a)
def _X_src_poster(a): return _X_("r", [b"src", b"poster"], a)
def _X_data(a):       return _X_("r", [b"data"],           a)
def _X_icon(a):       return _X_("r", [b"icon"],           a)

def _X_href(a):       return _X_("h", [b"href"],           a)
def _X_action(a):     return _X_("h", [b"action"],         a)
def _X_formaction(a): return _X_("h", [b"formaction"],     a)
def _X_cite(a):       return _X_("h", [b"cite"],           a)

# srcset is a comma-separated list of "image candidate strings", each
# consisting of a URL possibly followed by spaces and then "width" or
# "density" descriptors.  Note that leading spaces in each field of
# the comma-separated list are to be discarded, i.e. in srcset=" 1x",
# "1x" is the URL, not a descriptor.
def _X_src_srcset(a):
    urls = []
    v = _get_htmlattr(a, b"src")
    if v is not None: urls.append(_Sstrip(v))
    v = _get_htmlattr(a, b"srcset")
    if v is not None:
        for ic in v.split(","):
            ic = _SRE.split(_Sstrip(ic))
            if ic:
                urls.append(ic[0])

    return ("r", urls)

# link[href] is either "r", "h", or to be ignored depending on the value of
# the rel= property.
def _X_link_href(a):
    href = _get_htmlattr(a, b"href")
    rel  = _get_htmlattr(a, b"rel")
    if not href or not rel:
        return ("r", [])

    rel = _SRE.split(rel)
    for ty in ("icon", "pingback", "prefetch", "stylesheet"):
        if ty in rel:
            return ("r", [_Sstrip(href)])

    for ty in ("alternate", "author", "help", "license",
               "next", "prev", "search", "sidebar"):
        if ty in rel:
            return ("h", [_Sstrip(href)])

    return ("r", [])

# All elements that may be hyperlinks or resources, and how to extract the
# URLs.
_links = {
    # resources
    "audio":      _X_src,
    "embed":      _X_src,
    "iframe":     _X_src,
    "img":        _X_src_srcset,
    "input":      _X_src,
    "script":     _X_src,
    "source":     _X_src,
    "track":      _X_src,
    "video":      _X_src_poster,
    "object":     _X_data,
    "menuitem":   _X_icon,

    # hyperlinks
    "a":          _X_href,
    "area":       _X_href,
    "form":       _X_action,
    "button":     _X_formaction,
    "input":      _X_formaction,
    "blockquote": _X_cite,
    "del":        _X_cite,
    "ins":        _X_cite,
    "q":          _X_cite,

    # very special
    "link":       _X_link_href,
}

# All elements that (by default) do not display their children.
# <canvas> is excluded from this list because we want to capture
# its fallback content, if any.
_discards = frozenset((
    "audio",
    "embed",
    "head",
    "iframe",
    "img",
    "noframes",
    "noscript",
    "object",
    "script",
    "style",
    "template",
    "video",
))

# All elements that should NOT force a word break.
# For instance, "con<i>sis</i>tent" should produce "consistent", but
# "con<p>sis</p>tent" should produce "con sis tent".
_no_word_break = frozenset((
    "a",
    "abbr",
    "b",
    "basefont",
    "bdi",
    "bdo",
    "big",
    "blink",
    "cite",
    "code",
    "data",
    "del",
    "dfn",
    "em",
    "font",
    "i",
    "ins",
    "kbd",
    "malignmark",
    "mark",
    "mglyph",
    "mi",
    "mn",
    "mo",
    "ms",
    "mtext",
    "nobr",
    "plaintext",
    "q",
    "rb",
    "rp",
    "rt",
    "ruby",
    "s",
    "samp",
    "small",
    "span",
    "strike",
    "strong",
    "sub",
    "sup",
    "time",
    "tt",
    "u",
    "var",
))

class DomStatistics:
    def __init__(self):
        self.tags = collections.Counter()
        self.tags_at_depth = collections.Counter()

    def to_json(self):
        return { "tags"          : self.tags,
                 "tags_at_depth" : self.tags_at_depth }

class ExtractedContent:
    def __init__(self, url, page):
        self.url = url
        self.saw_base_href = False
        self.text_content = []
        self.links = []
        self.resources = []
        self.dom_stats = DomStatistics()

        with gumboc.parse(page) as output:
            self._process_document(output)

        self.text_content = _WSRE.sub(" ", "".join(self.text_content))
        self.links = sorted(set(self.links))
        self.resources = sorted(set(self.resources))

    def _process_document(self, output):

        root = output.contents.root.contents
        todo = collections.deque((iter((root,)),))
        path = collections.deque()
        discard = 0
        depth = 0

        while todo:
            sequence = todo[-1]
            try:
                node = next(sequence)

            except StopIteration:
                todo.pop()
                if path:
                    # close-tag
                    name = path.pop()
                    if name not in _no_word_break:
                        self.text_content.append(' ')
                    if name in _discards:
                        discard -= 1
                    depth -= 1

            else:
                ty = node.type.value
                data = node.contents

                if ty == 2 or ty == 3 or ty == 5:
                    # TEXT or CDATA or WHITESPACE
                    if not discard:
                        self.text_content.append(data.text.decode('utf-8'))

                elif ty == 1:
                    # element
                    name = data.tag_name
                    # Gumbo isn't really Py3K-safe
                    if hasattr(name, 'decode'):
                        name = name.decode('utf-8')

                    if name not in _no_word_break:
                        self.text_content.append(' ')

                    self.dom_stats.tags[name] += 1
                    self.dom_stats.tags_at_depth[depth] += 1

                    extractor = _links.get(name)
                    if extractor is not None:
                        ltype, targets = extractor(data.attributes)
                        if targets:
                            urls = [u for u in (urllib.parse.urljoin(self.url, t)
                                                for t in targets)
                                    if not _within_this_document(self.url, u)]
                            if ltype == "r":
                                self.resources.extend(urls)
                            else:
                                assert ltype == "h"
                                self.links.extend(urls)

                    # very special case for /html/head/base the first time it's seen
                    # (ignoring second and subsequent instances of <base href> is
                    # specified in HTML5)
                    if (name == "base" and depth == 2 and not self.saw_base_href
                        and path[-1] == "head" and path[-2] == "html"):
                        href = _get_htmlattr(data.attributes, b"href")
                        if href:
                            self.url = urllib.parse.urljoin(self.url, href)
                            self.saw_base_href = True

                    if data.children:
                        todo.append(iter(data.children))
                        path.append(name)
                        depth += 1
                        if name in _discards:
                            discard += 1

                else:
                    # <!doctype> or <!-- --> should be the only other things encountered
                    assert ty == 0 or ty == 4
