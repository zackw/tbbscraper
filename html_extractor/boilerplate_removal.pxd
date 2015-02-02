
from gumbo cimport GumboTag, GumboElement

# Classification of HTML elements.
# The labels are approximate.
ctypedef enum TagClass:
    TC_DISCARD = 0 # CSS default display:none
    TC_INLINE  = 1 # CSS default display:inline / HTML5 "phrasing content"
    TC_LINK    = 2 # <a>
    TC_BLOCK   = 3 # CSS default display:block
    TC_HEADING = 4 # <h*>, <hgroup>
    TC_PARA    = 5 # <p>, <blockquote>, <pre>, lists
    TC_GRAPHIC = 6 # treatment of graphics is context-dependent
    TC_ROOT    = 7 # <html>

cdef TagClass classify_tag(GumboTag tag)

# This check appears in a couple of places.
# <a> is not TC_INLINE but *doesn't* supply a word break.
cdef inline bint forces_word_break_p(TagClass cls):
    return cls != TC_INLINE and cls != TC_LINK and cls != TC_DISCARD

cdef class BlockTreeNode:
    # Internal state
    cdef list _textv
    cdef int  _depth
    cdef double _weight

    # Exposed state
    cdef readonly unicode text
    cdef readonly TagClass tagclass
    cdef readonly list children

    # Note: these are all 'double' because there's some weighting
    # which may cause any of them to take on non-integer values.
    cdef readonly double tagchars, \
                         textchars, \
                         totaltagchars, \
                         totaltextchars, \
                         textdensity, \
                         totaltextdensity

    cdef bint _dump_tree(self, outf, textwrap, int depth,
                         double thresh) except False

    cdef bint add_text(self, unicode text) except False
    cdef bint add_tag(self, unicode tagname, GumboElement* tag) except False
    cdef bint add_child(self, BlockTreeNode child) except False
    cdef bint finalize(self) except False

cdef class BlockTreeBuilder:
    cdef int depth
    cdef int in_discard
    cdef list stack     # list<BlockTreeNode> -- blocks under assembly
    cdef readonly BlockTreeNode tree


    cdef bint maybe_resume_prev_or_parent(self,
                                          unsigned int depth,
                                          bint is_heading) except False
    cdef bint enter_elt(self, TagClass tclass, unicode tname,
                        GumboElement* tag) except False
    cdef bint exit_elt(self, TagClass tclass) except False
    cdef bint add_text(self, unicode text) except False

cpdef object extract_content(BlockTreeNode tree)
