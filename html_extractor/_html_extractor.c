/* Extract content from HTML pages.  This is a wrapper around the
   Gumbo HTML5 parser library; for efficiency we need to do the tree
   walking as well as the parsing in C. */

#include <gumbo.h>

#include <Python.h>
#include <structmember.h>

/* Module global data. Mostly strings, some regexps. */

/* The HTML spec makes a distinction between "space characters"
   (perhaps better known as "ASCII whitespace": SPC TAB CR LF FF) and
   "White_Space characters" (everything considered whitespace by Unicode).
   Only "space characters" are considered whitespace inside attribute
   values. */

typedef struct he_global
{
  PyObject *empty_string;            // u""
  PyObject *one_space;               // u" "
  PyObject *sub_method;              // u"sub"
  PyObject *whitespace_character_re; // White_Space: /\s+/
  PyObject *space_character_re;      // ASCII space: /[ \t\r\n\f]+/

  // String for every GUMBO_TAG_* constant.
  PyObject *tag_names[GUMBO_TAG_LAST];
} he_global;
#define HE_GLOBAL(o) ((he_global *)PyModule_GetState(o))

/* This needs to be up here so that functions (other than
   PyInit_html_extractor) that need the globals can use
   HE_GLOBAL(PyState_FindModule(&html_extractor_module)).  */

static void traverse_html_extractor(PyObject *, visitproc, void *);
static void clear_html_extractor(PyObject *);

static struct PyModuleDef html_extractor_module = {
  PyModuleDef_HEAD_INIT
  "html_extractor",
  "Extraction of content from an HTML document.",
  .m_size     = sizeof(he_global),
  .m_traverse = traverse_html_extractor,
  .m_clear    = clear_html_extractor
};

#define HE_MODGLOBALS HE_GLOBAL(PyState_FindModule(&html_extractor_module))

/* String munging. */

static PyObject *
merge_text(he_global *G, PyObject *textvec)
{
  PyObject *merged = PyUnicode_Join(empty_string, textvec);
  if (!merged) return 0;

  PyObject *condensed =
    PyObject_CallMethodObjArgs(G->whitespace_character_re,
                               G->sub_method,
                               G->merged,
                               0);
  Py_DECREF(merged);
  return condensed;
}

/* Elements that introduce "heading content" according to HTML5.  Note
   that <header> does *not* count as "heading content".  (What we want
   here is the outline, not the page-header.)  */
static bool
is_heading(int tag)
{
  switch (tag) {
  case GUMBO_TAG_H1:
  case GUMBO_TAG_H2:
  case GUMBO_TAG_H3:
  case GUMBO_TAG_H4:
  case GUMBO_TAG_H5:
  case GUMBO_TAG_H6:
  case GUMBO_TAG_HGROUP:
    return true;
  default:
    return false;
  }
}

/* Elements that do not display their children.  <canvas> is excluded
   from this list because we want to capture its fallback content, if
   any. */
static bool
discards_contents(int tag)
{
  switch (tag) {
  case GUMBO_TAG_AUDIO:
  case GUMBO_TAG_EMBED:
  case GUMBO_TAG_HEAD:
  case GUMBO_TAG_IFRAME:
  case GUMBO_TAG_IMG:
  case GUMBO_TAG_NOFRAMES:
  case GUMBO_TAG_NOSCRIPT:
  case GUMBO_TAG_OBJECT:
  case GUMBO_TAG_SCRIPT:
  case GUMBO_TAG_STYLE:
  case GUMBO_TAG_TEMPLATE:
  case GUMBO_TAG_VIDEO:
    return true;
  default:
    return false;
  }
}

/* Elements that should NOT force a word break.  For instance,
   "con<i>sis</i>tent" should produce "consistent", but
   "con<p>sis</p>tent" should produce "con sis tent". */
static bool
forces_word_break(int tag)
{
  switch (tag) {
  case GUMBO_TAG_A:
  case GUMBO_TAG_ABBR:
  case GUMBO_TAG_B:
  case GUMBO_TAG_BASEFONT:
  case GUMBO_TAG_BDI:
  case GUMBO_TAG_BDO:
  case GUMBO_TAG_BIG:
  case GUMBO_TAG_BLINK:
  case GUMBO_TAG_CITE:
  case GUMBO_TAG_CODE:
  case GUMBO_TAG_DATA:
  case GUMBO_TAG_DEL:
  case GUMBO_TAG_DFN:
  case GUMBO_TAG_EM:
  case GUMBO_TAG_FONT:
  case GUMBO_TAG_I:
  case GUMBO_TAG_INS:
  case GUMBO_TAG_KBD:
  case GUMBO_TAG_MALIGNMARK:
  case GUMBO_TAG_MARK:
  case GUMBO_TAG_MGLYPH:
  case GUMBO_TAG_MI:
  case GUMBO_TAG_MN:
  case GUMBO_TAG_MO:
  case GUMBO_TAG_MS:
  case GUMBO_TAG_MTEXT:
  case GUMBO_TAG_NOBR:
  case GUMBO_TAG_PLAINTEXT:
  case GUMBO_TAG_Q:
  case GUMBO_TAG_RB:
  case GUMBO_TAG_RP:
  case GUMBO_TAG_RT:
  case GUMBO_TAG_RUBY:
  case GUMBO_TAG_S:
  case GUMBO_TAG_SAMP:
  case GUMBO_TAG_SMALL:
  case GUMBO_TAG_SPAN:
  case GUMBO_TAG_STRIKE:
  case GUMBO_TAG_STRONG:
  case GUMBO_TAG_SUB:
  case GUMBO_TAG_SUP:
  case GUMBO_TAG_TIME:
  case GUMBO_TAG_TT:
  case GUMBO_TAG_U:
  case GUMBO_TAG_VAR:
    return false;
  default:
    return true;
  }
}


/* Main tree walker.   Since this is now C rather than Python we are gonna
   just go ahead and use recursive function calls.

   All references in walker_state are borrowed. */

typedef struct walker_state
{
  unsigned int depth;
  unsigned int in_discard;
  unsigned int in_title;
  unsigned int in_heading;

  PyObject *title;
  PyObject *headings;
  PyObject *text_content;
  PyObject *tags;
  PyObject *tags_at_depth;

  he_global *G;
}
walker_state;

static bool
walk_text(const char *text, walker_state *state)
{
  if (state->in_discard && !state->in_title)
    return true;

  PyObject *s = PyUnicode_FromString(text);
  if (!s)
    return false;

  if (state->in_title)
    if (PyList_Append(state->title, s))
      goto fail;

  if (!state->in_discard) {
    if (PyList_Append(state->text_content, s))
      goto fail;

    if (state->in_heading) {
      PyObject *cur_heading = PyList_GET_ITEM(state->headings,
                                  PyList_GET_SIZE(state->headings) - 1);
      if (PyList_Append(cur_heading, s))
        goto fail;
    }
  }
  Py_DECREF(s);
  return true;

 fail:
  Py_DECREF(s);
  return false;
}

/* walk_element helper */
static bool
update_dom_stats(PyObject *tagname, walker_state *state)
{
  bool rv = false;
  PyObject *one   = PyLong_FromUnsignedLong(1);
  PyObject *depth = PyLong_FromUnsignedLong(state->depth);
  if (!one || !depth)
    goto out;

  PyObject *val = PyMapping_GetItem(state->tags_at_depth, depth);
  if (!val) {
    PyErr_Clear();
    val = PyLong_FromUnsignedLong(0);
  }
  if (!val)
    goto out;

  PyObject *sum = PyNumber_Add(val, one);
  if (!sum)
    goto out;
  if (PyMapping_SetItem(state->tags_at_depth, depth, sum))
    goto out;

  Py_DECREF(val);
  Py_DECREF(sum);

  val = PyMapping_GetItem(state->tags, tagname);
  if (!val) {
    PyErr_Clear();
    val = PyLong_FromUnsignedLong(0);
  }
  if (!val)
    goto out;

  PyObject *sum = PyNumber_Add(val, one);
  if (!sum)
    goto out;
  if (PyMapping_SetItem(state->tags, tagname, sum))
    goto out;

  rv = true;
 out:
  Py_XDECREF(sum);
  Py_XDECREF(val);
  Py_XDECREF(one);
  Py_XDECREF(depth);
  return rv;
}

static bool
walk_element(GumboElement *elt, GumboParseFlags flags, walker_state *state)
{
  /* Record only elements that appeared explicitly in the HTML. */
  if (flags == GUMBO_INSERTION_NORMAL ||
      flags == GUMBO_INSERTION_IMPLICIT_END_TAG) {
    if (elt->tag != GUMBO_TAG_UNKNOWN) {
      if (!update_dom_stats(state->G->tagnames[elt->tag], state))
        return false;

    } else {
      PyObject *tagname = 0;
      if (elt->tag_namespace == GUMBO_NAMESPACE_SVG) {
        const char *svg_tagname =
          gumbo_normalize_svg_tagname(&elt->original_name);
        if (svg_tagname) {
          tagname = PyUnicode_FromString(svg_tagname);
          if (!tagname)
            return false;
        }
      }
      if (!tagname) {
        tagname = PyUnicode_FromStringAndSize(elt->original_name.data,
                                              elt->original_name.length);
        if (!tagname)
          return false;
      }
      bool rv = update_dom_stats(tagname, state);
      Py_DECREF(tagname);
      if (!rv)
        return false;
    }
  }

  /* Empty elements may still force word breaks. */
  bool elt_forces_word_break = forces_word_break(elt->tag);
  if (elt_forces_word_break)
    if (!walk_text(" ", state))
      return false;

  if (elt->children.length == 0) /* empty element, we're done */
    return true;

  bool rv = false;
  bool elt_is_title          = elt->tag == GUMBO_TAG_TITLE;
  bool elt_is_heading        = is_heading(elt->tag);
  bool elt_discards_contents = discards_contents(elt->tag);

  if (elt_discards_contents)
    state->in_discard += 1;
  if (elt_is_title)
    state->in_title += 1;
  if (elt_is_heading) {
    state->in_heading += 1;
    if (state->in_heading == 1) {
      PyObject *heading = PyList_New(0);
      if (!heading)
        goto out;
      if (PyList_Append(state->headings, heading)) {
        Py_DECREF(heading);
        goto out;
      }
      Py_DECREF(heading);
    }
  }

  unsigned int len = elt->children.length;
  for (unsigned int i = 0; i < len; i++)
    if (walk_node(elt->children.data[i], state))
      goto out;

  if (elt_forces_word_break)
    if (!walk_text(" ", state))
      goto out;
  rv = true;
 out:
  if (elt_is_title)
    state->in_title -= 1;
  if (elt_is_heading)
    state->in_heading -= 1;
  if (elt_discards_contents)
    state->in_discard -= 1;
  return rv;
}

static bool
walk_node(GumboNode *node, walker_state *state)
{
  switch (node->type) {
  case GUMBO_NODE_ELEMENT:
    return walk_element(&node->v.element, node->parse_flags, state);

  case GUMBO_NODE_COMMENT:
    /* just discard */
    return true;

  case GUMBO_NODE_TEXT:
  case GUMBO_NODE_CDATA:
  case GUMBO_NODE_WHITESPACE:
    return walk_text(node->v.text.text, state);

    /* GUMBO_NODE_DOCUMENT should never occur. */
  default:
    PyErr_Format(PyExc_SystemError, "unable to process node of type %u",
                 node->type);
    return false;
  }
  return true;
}

/* Pythonic interface. */

typedef struct DomStatistics {
  PyObject_HEAD
  PyObject *tags;
  PyObject *tags_at_depth;
} DomStatistics;

static void
DomStatistics_dealloc(DomStatistics *self)
{
  Py_CLEAR(self->tags);
  Py_CLEAR(self->tags_at_depth);
  Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *
DomStatistics_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
  /* Enforce the absence of arguments. */
  static const char *const nokw[] = {0};
  if (!PyArg_ParseTupleAndKeywords(args, kw, "", nokw))
    return 0;

  DomStatistics *self = (DomStatistics *)type->tp_alloc(type, 0);
  if (!self)
    return 0;

  self->tags = PyDict_New();
  self->tags_at_depth = PyDict_New();
  if (!self->tags || !self->tags_at_depth) {
    Py_DECREF(self);
    return 0;
  }
  return (PyObject *)self;
}

static PyObject *
DomStatistics_to_json(DomStatistics *self)
{
  return Py_BuildValue("{sOsO}",
                       "tags",          self->tags,
                       "tags_at_depth", self->tags_at_depth);
}

static const PyMemberDef DomStatistics_members[] = {
  { "tags", T_OBJECT_EX, offsetof(DomStatistics, tags), READONLY,
    "Dictionary of counters.  Each key is an HTML tag that appeared at least "
    "once in the document, with its spelling normalized.  The corresponding "
    "value is the number of times that tag appeared. Implicit tags are not "
    "counted." },
  { "tags_at_depth", T_OBJECT_EX,
    offsetof(DomStatistics, tags_at_depth), READONLY,
    "Dictionary of counters.  Each key is a tree depth in the document, and "
    "the corresponding value is the number of times a tag appeared at that "
    "depth.  Depths containing only implicit tags are not counted." },
  { 0 }
};

static const PyMethodDef DomStatistics_methods = {
  { "to_json", (PyCFunction)DomStatistics_to_json, METH_NOARGS,
    "Return a dictionary version of this object, which can be passed to "
    "json.dump()." },
  { 0 }
};

static const PyType_Slot DomStatistics_slots[] = {
  { Py_tp_dealloc, DomStatistics_dealloc },
  { Py_tp_new,     DomStatistics_new },
  { Py_tp_methods, DomStatistics_methods },
  { Py_tp_members, DomStatistics_members },
  { 0, 0 }
}
static const PyType_Spec DomStatistics_spec = {
  "html_extractor.DomStatistics",
  "Statistics about the DOM structure.",
  sizeof(DomStatisticsType), 0,
  DomStatistics_slots
};


typedef struct ExtractedContent {
  PyObject_HEAD
  PyObject *url;
  PyObject *title;
  PyObject *headings;
  PyObject *text_content;
  PyObject *dom_stats;
} ExtractedContent;

static void
ExtractedContent_dealloc(ExtractedContent *self)
{
  Py_CLEAR(self->url);
  Py_CLEAR(self->title);
  Py_CLEAR(self->headings);
  Py_CLEAR(self->text_content);
  Py_CLEAR(self->dom_stats);
  Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *
ExtractedContent_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
  ExtractedContent *self = (ExtractedContent *)type->tp_alloc(type, 0);
  if (!self)
    return 0;

  self->url          = (Py_INCREF(Py_None), Py_None);
  self->title        = PyList_New(0);
  self->headings     = PyList_New(0);
  self->text_content = PyList_New(0);
  self->dom_stats    = ((DomStatistics *)
                        PyObject_CallObject((PyObject*)&DomStatisticsType, 0));
  if (!self->url || !self->title || !self->headings || !self->text_content
      || !self->dom_stats) {
    Py_DECREF(self);
    return 0;
  }
  return (PyObject *)self;
}

/* subroutines of ExtractedContent_init, not exposed to Python as methods */
static bool
process_document(ExtractedContent *self, Py_buffer *page)
{
}

static bool
postprocess_document(he_global *G, ExtractedContent *self)
{
  PyObject *merged, *old;

  old = self->text_content;
  merged = merge_text(G, old);
  if (!merged) return false;
  self->text_content = merged;
  Py_DECREF(old);

  old = self->title;
  merged = merge_text(G, old);
  if (!merged) return false;
  self->title = merged;
  Py_DECREF(old);

  for (Py_ssize_t i = 0, len = PyList_GET_SIZE(self->headings); i < len; i++) {
    old = PyList_GET_ITEM(state->headings, i);
    merged = merge_text(G, old);
    if (!merged) return false;
    PyList_SET_ITEM(state->headings, i, merged);
    Py_DECREF(old);
  }

  return true;
}

static int
ExtractedContent_init(ExtractedContent *self, PyObject *args, PyObject *kwds)
{
  PyObject *url;
  Py_buffer page;
  he_global *G = HE_MODGLOBALS;

  // Note: the 's*' format descriptor uses PyBUF_SIMPLE, so we are
  // guaranteed a contiguous buffer in page->buf.  The database is
  // known to contain UTF-8 only.

  static const char *const kwlist[] = { "url", "page", 0 };
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "Us*", kwlist,
                                   &url, &page))
    return -1;

  Py_CLEAR(self->url);
  self->url = url;
  Py_INCREF(self->url);

  GumboOptions opts        = kGumboDefaultOptions;
  opts.userdata            = 0;
  opts.stop_on_first_error = false;
  opts.max_errors          = 0;

  // Note: must hold onto the page buffer until after we're done
  // walking the gumbo tree, because the gumbo tree contains pointers
  // into the page buffer.
  GumboOutput *output = gumbo_parse_with_options(&opts, page->buf, page->len);
  bool success = false;
  if (output) {
    walker_state state;
    state.G             = G;
    state.depth         = 0;
    state.in_discard    = 0;
    state.in_title      = 0;
    state.in_heading    = 0;
    state.title         = self->title;
    state.headings      = self->headings;
    state.text_content  = self->text_content;
    state.tags          = self->dom_stats->tags;
    state.tags_at_depth = self->dom_stats->tags_at_depth;

    success = walk_node(output->root, &state);
  }
  gumbo_destroy_output(&opts, output);
  PyBuffer_Release(&page);
  if (!success)
    return -1;

  success = postprocess_document(G, self);
  return success ? 0 : -1;
}

#define EC_MEMBER(s) #s, T_OBJECT_EX, offsetof(ExtractedContent, s), READONLY
static const PyMemberDef ExtractedContent_members = {
  { EC_MEMBER(url),
    "String: URL of the page.  If <base href> was used, will reflect that." },
  { EC_MEMBER(title),
    "String: Title of the page, i.e. the contents of the <title> element "
    "if any." },
  { EC_MEMBER(headings),
    "Array of strings: text of all the headings on the page, one string per "
    "outermost <hN> or <hgroup> element." },
  { EC_MEMBER(text_content),
    "String: all text content on the page. Includes the text of the headings, "
    "but not the title." },
  { EC_MEMBER(dom_stats),
    "DomStatistics object recording statistics about the DOM structure of "
    "this page." },
  { 0 }
};

/* there are no non-special methods */

static const PyType_Slot ExtractedContent_slots[] = {
  { Py_tp_dealloc, ExtractedContent_dealloc },
  { Py_tp_new,     ExtractedContent_new },
  { Py_tp_init,    ExtractedContent_init },
  { Py_tp_methods, ExtractedContent_methods },
  { 0, 0 }
};
static const PyType_Spec ExtractedContent_spec = {
  "html_extractor.ExtractedContent",
  "Content extracted from an HTML document.",
  sizeof(ExtractedContentType), 0,
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
  ExtractedContent_slots
};

/* Module setup and teardown */

static bool
init_he_global(he_global *G)
{
  PyObject *re = PyImport_ImportModule("re");
  if (!re)
    return false;
  PyObject *re_compile = PyObject_GetAttrString(re, "compile");
  if (!re_compile) {
    Py_DECREF(re);
    return false;
  }

  G->space_character_re =
    PyObject_CallFunction(re_compile, "s", "[ \\t\\r\\n\\f]+");

  G->whitespace_character_re =
    PyObject_CallFunction(re_compile, "s", "\\s+");

  Py_DECREF(re_compile);
  Py_DECREF(re);

  if (!G->space_character_re || !G->whitespace_character_re)
    return false;

  G->empty_string = PyUnicode_FromString("");
  G->one_space    = PyUnicode_FromString(" ");
  G->sub_method   = PyUnicode_FromString("sub");

  if (!G->empty_string || !G->one_space || !G->sub_method)
    return false;

  for (int i = 0; i < GUMBO_TAG_LAST; i++) {
    if (i == GUMBO_TAG_UNKNOWN)
      continue;

    const char *name = gumbo_normalized_tagname(i);
    if (!name) return false;
    PyObject *py_name = PyUnicode_FromString(name);
    if (!py_name) return false;
    G->tagnames[i] = py_name;
  }
  return true;
}

static void
traverse_html_extractor(PyObject *m, visitproc v, void *arg)
{
  he_global *G = HE_GLOBAL(m);

  Py_VISIT(G->empty_string);
  Py_VISIT(G->one_space);
  Py_VISIT(G->sub_method);
  Py_VISIT(G->whitespace_character_re);
  Py_VISIT(G->space_character_re);

  for (int i = 0; i < GUMBO_TAG_LAST; i++)
    Py_VISIT(G->tagnames[i]);
}

static void
clear_html_extractor(PyObject *m)
{
  he_global *G = HE_GLOBAL(m);

  Py_CLEAR(G->empty_string);
  Py_CLEAR(G->one_space);
  Py_CLEAR(G->sub_method);
  Py_CLEAR(G->whitespace_character_re);
  Py_CLEAR(G->space_character_re);

  for (int i = 0; i < GUMBO_TAG_LAST; i++)
    Py_CLEAR(G->tagnames[i]);
}

PyMODINIT_FUNC
PyInit_html_extractor(void)
{
  PyObject *mod = 0, *tp = 0;

  mod = PyModule_Create(&html_extractor_module);
  if (!mod) goto fail;
  if (!init_he_global(HE_GLOBAL(mod))) goto fail;

  tp = PyType_FromSpec(&DomStatistics_spec);
  if (!tp) goto fail;
  if (PyModule_AddObject(mod, "DomStatistics", tp)) goto fail;

  tp = PyType_FromSpec(&ExtractedContent_spec);
  if (!tp) goto fail;
  if (PyModule_AddObject(mod, "ExtractedContent", tp)) goto fail;

  return mod;

 fail:
  /* If PyModule_AddObject fails, we still hold a reference to tp. */
  Py_XDECREF(tp);
  Py_XDECREF(mod);
  return 0;
}
