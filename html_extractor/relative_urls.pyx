"""Relative URL resolution.  Originally based on urllib.parse, with
   everything except urljoin, urlparse, urlunparse stripped out, and
   Cythonified for speed.

"""

__all__ = (u'urljoin', u'urljoin_outbound')

# A classification of schemes
uses_relative = frozenset((
    u'file', u'ftp', u'gopher', u'http', u'https', u'imap', u'mms',
    u'nntp', u'prospero', u'rtsp', u'rtspu', u'sftp', u'shttp',
    u'svn', u'svn+ssh', u'wais',
))
uses_netloc = frozenset((
    u'file', u'ftp', u'git', u'git+ssh', u'gopher', u'http', u'https',
    u'imap', u'mms', u'nfs', u'nntp', u'prospero', u'rsync', u'rtsp',
    u'rtspu', u'sftp', u'shttp', u'snews', u'svn', u'svn+ssh',
    u'telnet', u'wais',
))
uses_params = frozenset((
    u'ftp', u'hdl', u'http', u'https', u'imap', u'mms', u'prospero',
    u'rtsp', u'rtspu', u'sftp', u'shttp', u'sip', u'sips', u'tel',
))

# Characters valid in scheme names
DEF scheme_chars = (u'abcdefghijklmnopqrstuvwxyz'
                    u'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                    u'0123456789'
                    u'+-.')

cdef inline tuple _splitnetloc(unicode url):
    cdef unsigned int i
    i = 0
    for c in url:
        if i >= 2 and c in u'/?#':
            return url[2:i], url[i:]
        i += 1
    return url[2:], u''

cdef inline tuple _splitparams(unicode url):
    cdef Py_ssize_t i
    i = url.rfind(u'/')
    if i >= 0:
        i = url.find(u';', i+1)
    else:
        i = url.find(u';')
    if i < 0:
        return url, u''
    else:
        return url[:i], url[i+1:]

cdef inline tuple _splitchar(unicode url, unicode c):
    cdef Py_ssize_t i = url.rfind(c)
    if i >= 0:
        return url[:i], url[i+1:]
    return url, u''

cdef tuple urlparse(unicode url, unicode scheme=u''):
    cdef Py_ssize_t i
    cdef unicode netloc   = u''
    cdef unicode params   = u''
    cdef unicode query
    cdef unicode fragment

    i = url.find(u':')
    if i > 0:
        if url[:i] in (u'http', u'https'): # optimize the common case
            scheme = url[:i]
            url = url[i+1:]
        else:
            maybe_scheme = url[:i]
            for c in maybe_scheme:
                if c not in scheme_chars:
                    break
            else:
                # make sure "url" is not actually a port number (in which case
                # "scheme" is really part of the path)
                rest = url[i+1:]
                if not rest:
                    scheme, url = maybe_scheme.lower(), rest
                else:
                    for c in rest:
                        if c not in u'0123456789':
                            # not a port number
                            scheme, url = maybe_scheme.lower(), rest
                            break

    if url[:2] == u'//':
        netloc, url = _splitnetloc(url)

    url, fragment = _splitchar(url, u'#')
    url, query    = _splitchar(url, u'?')
    if scheme in uses_params:
        url, params = _splitparams(url)

    return (scheme, netloc, url, params, query, fragment)

cdef unicode urlunparse(unicode scheme,
                        unicode netloc,
                        unicode path,
                        unicode params,
                        unicode query,
                        unicode fragment):
    cdef unicode url = path
    if netloc or (not url.startswith(u'//') and scheme in uses_netloc):
        if not url.startswith(u'/'):
            url = u'/' + url
        url = u'//' + netloc + url
    if scheme:
        url = scheme + u':' + url
    if params:
        url = url + u';' + params
    if query:
        url = url + u'?' + query
    if fragment:
        url = url + u'#' + fragment
    return url

cpdef unicode urljoin(unicode base, unicode url):
    """Join a base URL and a possibly relative URL to form an absolute
    interpretation of the latter."""
    cdef unicode bscheme, bnetloc, bpath, bparams, bquery, bfragment
    cdef unicode scheme, netloc, path, params, query, fragment
    cdef Py_ssize_t i, n

    if not base:
        scheme, netloc, path, params, query, fragment = urlparse(url, u'')
        return urlunparse(scheme, netloc, path, params, query, fragment)
    if not url:
        bscheme, bnetloc, bpath, bparams, bquery, bfragment = urlparse(base, u'')
        return urlunparse(bscheme, bnetloc, bpath, bparams, bquery, bfragment)

    bscheme, bnetloc, bpath, bparams, bquery, bfragment = urlparse(base, u'')
    scheme, netloc, path, params, query, fragment       = urlparse(url, bscheme)
    if scheme != bscheme or scheme not in uses_relative:
        return urlunparse(scheme, netloc, path, params, query, fragment)
    if scheme in uses_netloc:
        if netloc:
            return urlunparse(scheme, netloc, path, params, query, fragment)
        netloc = bnetloc
    if path.startswith(u'/'):
        return urlunparse(scheme, netloc, path, params, query, fragment)

    if not path and not params:
        path = bpath
        params = bparams
        if not query:
            query = bquery
        return urlunparse(scheme, netloc, path, params, query, fragment)

    segments = bpath.split(u'/')[:-1] + path.split(u'/')
    # XXX The stuff below is bogus in various ways...
    if segments[-1] == u'.':
        segments[-1] = u''
    while u'.' in segments:
        segments.remove(u'.')
    while 1:
        i = 1
        n = len(segments) - 1
        while i < n:
            if (segments[i] == u'..'
                and segments[i-1] not in (u'', u'..')):
                del segments[i-1:i+1]
                break
            i = i+1
        else:
            break
    if segments == [u'', u'..']:
        segments[-1] = u''
    elif len(segments) >= 2 and segments[-1] == u'..':
        segments[-2:] = [u'']
    return urlunparse(scheme, netloc, u'/'.join(segments),
                      params, query, fragment)

cpdef unicode urljoin_outbound(unicode doc, unicode url):
    """If URL is the same as DOCURL, or a link to an anchor within DOCURL,
       return None.  Otherwise, return urljoin(doc, url)."""
    dest = urljoin(doc, url)
    (doc, _) = _splitchar(doc, u'#')
    (dpage, _) = _splitchar(dest, u'#')
    if dpage != doc: return None
    return dest
