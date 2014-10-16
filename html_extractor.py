#! /usr/bin/python3

# Extract content from HTML pages.  Inspired by html2text.py, but this
# is a from-scratch implementation based on html5lib.  Does not
# attempt to produce Markdown; just extracts the text, and lists of
# outbound links and resource references.

import html5lib
import urllib.parse
import re
import sys

_HTML_NS = "http://www.w3.org/1999/xhtml"

_Walker = html5lib.getTreeWalker("etree")

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
    v = attrs.get(name)
    if v: return v
    v = attrs.get((None, name))
    if v: return v
    v = attrs.get((_HTML_NS, name))
    if v: return v
    return None

def _X_(mode, attrs, a):
    urls = []
    for at in attrs:
        v = _get_htmlattr(a, at)
        if v is not None: urls.append(_Sstrip(v))
    return (mode, urls)

def _X_src(a):        return _X_("r", ["src"],           a)
def _X_src_poster(a): return _X_("r", ["src", "poster"], a)
def _X_data(a):       return _X_("r", ["data"],          a)
def _X_icon(a):       return _X_("r", ["icon"],          a)

def _X_href(a):       return _X_("h", ["href"],          a)
def _X_action(a):     return _X_("h", ["action"],        a)
def _X_formaction(a): return _X_("h", ["formaction"],    a)
def _X_cite(a):       return _X_("h", ["cite"],          a)

# srcset is a comma-separated list of "image candidate strings", each
# consisting of a URL possibly followed by spaces and then "width" or
# "density" descriptors.  Note that leading spaces in each field of
# the comma-separated list are to be discarded, i.e. in srcset=" 1x",
# "1x" is the URL, not a descriptor.
def _X_src_srcset(a):
    urls = []
    v = _get_htmlattr(a, "src")
    if v is not None: urls.append(_Sstrip(v))
    v = _get_htmlattr(a, "srcset")
    if v is not None:
        for ic in v.split(","):
            ic = _SRE.split(_Sstrip(ic))
            if ic:
                urls.append(ic[0])

    return ("r", urls)

# link[href] is either "r", "h", or to be ignored depending on the value of
# the rel= property.
def _X_link_href(a):
    href = _get_htmlattr(a, "href")
    rel  = _get_htmlattr(a, "rel")
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

class ExtractedContent:
    def __init__(self, url, page):
        document = html5lib.parse(page)
        walker = _Walker(document)
        base = document.find("./h:head/h:base[@href]",
                             namespaces={'h':_HTML_NS})
        if base:
            href = _SRE.sub("", base.get("href", ""))
            self.url = urllib.parse.urljoin(url, href)
        else:
            self.url = url

        self.text_content = []
        self.links = []
        self.resources = []

        self._process_document(walker)

        self.text_content = _WSRE.sub(" ", "".join(self.text_content))
        self.links = sorted(set(self.links))
        self.resources = sorted(set(self.resources))

    def _process_document(self, walker):

        discard = 0

        for token in walker:
            t = token["type"]
            if t in ("Doctype", "Comment"):
                pass
            elif t in ("StartTag", "EmptyTag"):
                name = token["name"]
                if name in _discards and t != "EmptyTag":
                    discard += 1

                extractor = _links.get(name)
                if extractor is not None:
                    ltype, urls = extractor(token["data"])
                    if urls:
                        urls = [urllib.parse.urljoin(self.url, u)
                                for u in urls]
                        urls = [u for u in urls
                                if not _within_this_document(self.url, u)]
                        if ltype == "r":
                            self.resources.extend(urls)
                        else:
                            assert ltype == "h"
                            self.links.extend(urls)

            elif t == "EndTag":
                name = token["name"]
                if name in _discards:
                    discard -= 1

            elif t in ("Characters", "SpaceCharacters"):
                if not discard:
                    self.text_content.append(token["data"])

            else:
                raise RuntimeError("Unexpected token: {!r}".format(token))
