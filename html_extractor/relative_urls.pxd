"""Relative URL resolution.  Originally based on urllib.parse, with
   everything except urljoin, urlparse, urlunparse stripped out, and
   Cythonified for speed.

"""

cpdef unicode urljoin(unicode base, unicode url)
cpdef unicode urljoin_outbound(unicode doc, unicode url)
