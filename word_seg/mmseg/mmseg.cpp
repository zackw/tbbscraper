/*
* German M. Bravo (Kronuz) <german.mb@gmail.com>
*
* MIT license (http://www.opensource.org/licenses/mit-license.php)
* Copyright (c) 2011 German M. Bravo (Kronuz), All rights reserved.
* Python3 conversion 2015, Zack Weinberg.
*/
#include <Python.h>
#include <structmember.h>
#include <unicodeobject.h>

#include "utils.h"
#include "token.h"
#include "dict.h"
#include "algor.h"


/* Dictionary */

typedef struct {
	PyObject_HEAD
} mmseg_Dictionary;

static PyObject *
mmseg_Dictionary_load_chars(PyObject *self, PyObject *args)
{
	char *path;
	if (!PyArg_ParseTuple(args, "s", &path))
		return NULL;

	if (rmmseg::dict::load_chars(path)) {
		Py_INCREF(Py_True);
		return (PyObject *)Py_True;
	} else {
		Py_INCREF(Py_False);
		return (PyObject *)Py_False;
	}
}

static PyObject *
mmseg_Dictionary_load_words(PyObject *self, PyObject *args)
{
	char *path;
	if (!PyArg_ParseTuple(args, "s", &path))
		return NULL;

	if (rmmseg::dict::load_words(path)) {
		Py_INCREF(Py_True);
		return (PyObject *)Py_True;
	} else {
		Py_INCREF(Py_False);
		return (PyObject *)Py_False;
	}
}

static PyObject *
mmseg_Dictionary_add(PyObject *self, PyObject *args, PyObject *kwds)
{
	char *utf8 = NULL;
	int ulen, chars = -1, freq = 0;

	static const char *kwlist[] = {"word", "chars", "freq", NULL};

	if (!PyArg_ParseTupleAndKeywords(args, kwds, "s#|ii", (char **)(kwlist), &utf8, &ulen, &chars, &freq))
		return NULL;

	if (chars == -1)
		chars = ulen;

	rmmseg::Word *w = rmmseg::make_word(utf8, chars, freq, ulen);
	rmmseg::dict::add(w);

	Py_INCREF(Py_None);
	return (PyObject *)Py_None;
}

static PyObject *
mmseg_Dictionary_has_word(PyObject *self, PyObject *args)
{
	char *utf8 = NULL;
	int ulen;

	if (!PyArg_ParseTuple(args, "s#", &utf8, &ulen))
		return NULL;

	if (rmmseg::dict::get(utf8, ulen)) {
		Py_INCREF(Py_True);
		return (PyObject *)Py_True;
	} else {
		Py_INCREF(Py_False);
		return (PyObject *)Py_False;
	}
}

static PyMethodDef mmseg_Dictionary_methods[] = {
	{"load_chars", (PyCFunction)mmseg_Dictionary_load_chars, METH_VARARGS | METH_STATIC, "Load a characters dictionary from a file."},
	{"load_words", (PyCFunction)mmseg_Dictionary_load_words, METH_VARARGS | METH_STATIC, "Load a words dictionary from a file."},
	{"add", (PyCFunction)mmseg_Dictionary_add, METH_VARARGS | METH_KEYWORDS | METH_STATIC, "Add a word to the in-memory dictionary."},
	{"has_word", (PyCFunction)mmseg_Dictionary_has_word, METH_VARARGS | METH_STATIC, "Check whether one word is included in the dictionary."},
	{NULL, NULL, 0, NULL}        /* Sentinel */
};


static PyTypeObject mmseg_DictionaryType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"mmseg.Dictionary",                        /* tp_name */
	sizeof(mmseg_Dictionary),                  /* tp_basicsize */
	0,                                         /* tp_itemsize */
	0,                                         /* tp_dealloc */
	0,                                         /* tp_print */
	0,                                         /* tp_getattr */
	0,                                         /* tp_setattr */
	0,                                         /* tp_reserved */
	0,                                         /* tp_repr */
	0,                                         /* tp_as_number */
	0,                                         /* tp_as_sequence */
	0,                                         /* tp_as_mapping */
	0,                                         /* tp_hash */
	0,                                         /* tp_call */
	0,                                         /* tp_str */
	0,                                         /* tp_getattro */
	0,                                         /* tp_setattro */
	0,                                         /* tp_as_buffer */
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,  /* tp_flags */
	"MMSeg Dictionary.",                       /* tp_doc */
	0,                                         /* tp_traverse */
	0,                                         /* tp_clear */
	0,                                         /* tp_richcompare */
	0,                                         /* tp_weaklistoffset */
	0,                                         /* tp_iter: __iter__() method */
	0,                                         /* tp_iternext: next() method */
	mmseg_Dictionary_methods,                  /* tp_methods */
	0,                                         /* tp_members */
	0,                                         /* tp_getset */
	0,                                         /* tp_base */
	0,                                         /* tp_dict */
	0,                                         /* tp_descr_get */
	0,                                         /* tp_descr_set */
	0,                                         /* tp_dictoffset */
	0,                                         /* tp_init */
	0,                                         /* tp_alloc */
	0                                          /* tp_new */
};


/* Token */

typedef struct {
	PyObject_HEAD
	PyObject *text;
	int start;
	int end;
	int length;
} mmseg_Token;

static PyObject *
mmseg_Token_str(PyObject* self_)
{
	mmseg_Token* self = reinterpret_cast<mmseg_Token*>(self_);
	Py_INCREF(self->text);
	return self->text;
}

static PyObject *
mmseg_Token_repr(PyObject* self_)
{
	mmseg_Token* self = reinterpret_cast<mmseg_Token*>(self_);
	return PyUnicode_FromFormat("<Token %d..%d %R>", self->start, self->end, self->text);
}

static void
mmseg_Token_dealloc(PyObject* self_)
{
	mmseg_Token* self = reinterpret_cast<mmseg_Token*>(self_);
	Py_XDECREF(self->text);
	Py_TYPE(self_)->tp_free(self_);
}

static PyObject *
mmseg_Token_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
	mmseg_Token *self;

	self = (mmseg_Token *)type->tp_alloc(type, 0);
	if (!self)
		return NULL;

	self->text = PyUnicode_New(0, 0);
	if (!self->text) {
		Py_DECREF(self);
		return NULL;
	}

	self->start = 0;
	self->end = 0;
	self->length = 0;
	return reinterpret_cast<PyObject *>(self);
}

static int
mmseg_Token_init(PyObject *self_, PyObject *args, PyObject *kwds)
{
	mmseg_Token* self = reinterpret_cast<mmseg_Token*>(self_);
	PyObject *obj = NULL, *uni = NULL, *tmp;
	static const char *kwlist[] = {"text", "start", NULL};

	if (!PyArg_ParseTupleAndKeywords(args, kwds, "|Oi", (char **)(kwlist), &obj, &self->start)) {
		return -1;
	}

	if (obj) {
		if (PyUnicode_Check(obj)) {
			uni = obj;
			Py_INCREF(uni);
		} else {
			uni = PyUnicode_FromEncodedObject(obj, "utf-8", "strict");
			if (!uni)
				return -1;
		}
		if (PyUnicode_READY(uni)) {
			Py_DECREF(uni);
			return -1;
		}

		tmp = self->text;
		self->text = uni;
		self->length = static_cast<int>(PyUnicode_GET_LENGTH(self->text));
		self->end = self->start + self->length;
		Py_XDECREF(tmp);
	}

	return 0;
}

static PyMemberDef mmseg_Token_members[] = {
	{(char *)"text",   T_OBJECT_EX, offsetof(mmseg_Token, text),   READONLY},
	{(char *)"start",  T_INT,       offsetof(mmseg_Token, start),  READONLY},
	{(char *)"end",    T_INT,       offsetof(mmseg_Token, end),    READONLY},
	{(char *)"length", T_INT,       offsetof(mmseg_Token, length), READONLY},
	{NULL}        /* Sentinel */
};

/* Type definition */

static PyTypeObject mmseg_TokenType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"mmseg.Token",                             /* tp_name */
	sizeof(mmseg_Token),                       /* tp_basicsize */
	0,                                         /* tp_itemsize */
	mmseg_Token_dealloc,                       /* tp_dealloc */
	0,                                         /* tp_print */
	0,                                         /* tp_getattr */
	0,                                         /* tp_setattr */
	0,                                         /* tp_reserved */
	mmseg_Token_repr,                          /* tp_repr */
	0,                                         /* tp_as_number */
	0,                                         /* tp_as_sequence */
	0,                                         /* tp_as_mapping */
	0,                                         /* tp_hash */
	0,                                         /* tp_call */
	mmseg_Token_str,                           /* tp_str */
	0,                                         /* tp_getattro */
	0,                                         /* tp_setattro */
	0,                                         /* tp_as_buffer */
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,  /* tp_flags */
	"MMSeg Token object.",                     /* tp_doc */
	0,                                         /* tp_traverse */
	0,                                         /* tp_clear */
	0,                                         /* tp_richcompare */
	0,                                         /* tp_weaklistoffset */
	0,                                         /* tp_iter: __iter__() method */
	0,                                         /* tp_iternext: next() method */
	0,                                         /* tp_methods */
	mmseg_Token_members,                       /* tp_members */
	0,                                         /* tp_getset */
	0,                                         /* tp_base */
	0,                                         /* tp_dict */
	0,                                         /* tp_descr_get */
	0,                                         /* tp_descr_set */
	0,                                         /* tp_dictoffset */
	mmseg_Token_init,                          /* tp_init */
	0,                                         /* tp_alloc */
	mmseg_Token_new                            /* tp_new */
};


/* Algorithm */

typedef struct {
	PyObject_HEAD
	char *text;
	Py_ssize_t pos;
	rmmseg::Algorithm *algorithm;
} mmseg_Algorithm;

static int
mmseg_Algorithm_init(PyObject *self_, PyObject *args, PyObject *kwds)
{
	mmseg_Algorithm* self = reinterpret_cast<mmseg_Algorithm*>(self_);
	char *utf8 = NULL;
	int ulen;

	if (!PyArg_ParseTuple(args, "s#", &utf8, &ulen)) {
		return -1;
	}

	self->pos = 0;
	self->text = PyMem_Strdup(utf8);
	self->algorithm = new rmmseg::Algorithm(self->text, ulen);
	return 0;
}

static void
mmseg_Algorithm_dealloc(PyObject *self_)
{
	mmseg_Algorithm* self = reinterpret_cast<mmseg_Algorithm*>(self_);
	if (self->text) PyMem_Del(self->text);
	if (self->algorithm) delete self->algorithm;

	Py_TYPE(self_)->tp_free(self_);
}

PyObject*
mmseg_Algorithm_iter(PyObject *self_)
{
	Py_INCREF(self_);
	return self_;
}

PyObject*
mmseg_Algorithm_iternext(PyObject *self_)
{
	mmseg_Algorithm* self = reinterpret_cast<mmseg_Algorithm*>(self_);
	rmmseg::Token rtk = self->algorithm->next_token();
	if (!rtk.text) {
		/* Raising of standard StopIteration exception with empty value. */
		PyErr_SetNone(PyExc_StopIteration);
		return NULL;
	}

	mmseg_Token *result = PyObject_New(mmseg_Token, &mmseg_TokenType);
	if (!result)
		return NULL;
	if (!(result->text = PyUnicode_DecodeUTF8(rtk.text, rtk.length, "strict"))) {
		Py_DECREF(result);
		return NULL;
	}
	result->start = self->pos;
	result->length = PyUnicode_GET_LENGTH(result->text);
	result->end = result->start + result->length;
	self->pos += result->length;
	return reinterpret_cast<PyObject*>(result);
}

/* Type definition */

static PyTypeObject mmseg_AlgorithmType = {
	PyVarObject_HEAD_INIT(NULL,0)
	"mmseg.Algorithm",                   /* tp_name */
	sizeof(mmseg_Algorithm),             /* tp_basicsize */
	0,                                   /* tp_itemsize */
	mmseg_Algorithm_dealloc,             /* tp_dealloc */
	0,                                   /* tp_print */
	0,                                   /* tp_getattr */
	0,                                   /* tp_setattr */
	0,                                   /* tp_compare */
	0,                                   /* tp_repr */
	0,                                   /* tp_as_number */
	0,                                   /* tp_as_sequence */
	0,                                   /* tp_as_mapping */
	0,                                   /* tp_hash  */
	0,                                   /* tp_call */
	0,                                   /* tp_str */
	0,                                   /* tp_getattro */
	0,                                   /* tp_setattro */
	0,                                   /* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,                  /* tp_flags */
	"MMSeg Algorithm iterator object.",  /* tp_doc */
	0,                                   /* tp_traverse */
	0,                                   /* tp_clear */
	0,                                   /* tp_richcompare */
	0,                                   /* tp_weaklistoffset */
	mmseg_Algorithm_iter,                /* tp_iter: __iter__() method */
	mmseg_Algorithm_iternext,            /* tp_iternext: next() method */
	0,                                   /* tp_methods */
	0,                                   /* tp_members */
	0,                                   /* tp_getset */
	0,                                   /* tp_base */
	0,                                   /* tp_dict */
	0,                                   /* tp_descr_get */
	0,                                   /* tp_descr_set */
	0,                                   /* tp_dictoffset */
	mmseg_Algorithm_init,                /* tp_init */
};


/* Module definition */

static PyModuleDef mmseg_module = {
	PyModuleDef_HEAD_INIT,
	"_mmseg",
	"Word segmentation for Chinese.",
	-1, /* There is global state inside rmmseg. */
	NULL, NULL, NULL, NULL, NULL
};

/* Module init function */

PyMODINIT_FUNC
PyInit__mmseg(void)
{
	PyObject* m = PyModule_Create(&mmseg_module);
	if (!m)
		return NULL;

	Py_INCREF(&mmseg_DictionaryType);
	if (PyType_Ready(&mmseg_DictionaryType) < 0 ||
	    PyModule_AddObject(m, "Dictionary", (PyObject *)&mmseg_DictionaryType))
		goto fail;

	Py_INCREF(&mmseg_TokenType);
	if (PyType_Ready(&mmseg_TokenType) < 0 ||
	    PyModule_AddObject(m, "Token", (PyObject *)&mmseg_TokenType))
		goto fail;

        mmseg_AlgorithmType.tp_new = PyType_GenericNew;
	Py_INCREF(&mmseg_AlgorithmType);
	if (PyType_Ready(&mmseg_AlgorithmType) < 0 ||
	    PyModule_AddObject(m, "Algorithm", (PyObject *)&mmseg_AlgorithmType))
		goto fail;

	return m;

 fail:
	Py_XDECREF(m);
	return NULL;
}
