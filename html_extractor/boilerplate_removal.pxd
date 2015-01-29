
from gumbo cimport GumboTag

# Classification of HTML elements.
# The labels are approximate.
ctypedef enum TagClass:
    TC_DISCARD = 0 # CSS default display:none
    TC_BLOCK   = 1 # CSS default display:block
    TC_INLINE  = 2 # CSS default display:inline / HTML5 "phrasing content"
    TC_LINK    = 3 # <a> is a special case
    TC_HEADING = 4 # <h*> is a special case (but <header> is not)
    TC_PARA    = 5 # <p>, <blockquote>, <pre>, lists,
    TC_GRAPHIC = 6 # treatment of graphics is context-dependent

cdef TagClass classify_tag(GumboTag tag)
