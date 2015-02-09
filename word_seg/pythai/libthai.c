/* Originally part of pythai: https://pypi.python.org/pypi/pythai
   Simplified and Python3-ified 2015, Zack Weinberg */

#include <Python.h>

#include <thai/thwchar.h>
#include <thai/thwbrk.h>

static PyObject*
split_(PyObject *self, PyObject *text)
{
    PyObject  *result = NULL;
    PyObject  *tok    = NULL;
    int       *breaks = NULL;
    thwchar_t *buffer = NULL;
    int i, s, n, len;

    if (!PyUnicode_Check(text)) {
        PyErr_SetString(PyExc_TypeError, "a string is required");
        return NULL;
    }
    if (PyUnicode_READY(text))
        return NULL;

    len = PyUnicode_GET_LENGTH(text);
    if (len == 0)
        return PyList_New(0);

    breaks = PyMem_New(int, len);
    if (!breaks)
        goto nomem;

    buffer = PyUnicode_AsWideCharString(text, 0);
    if (!buffer)
        goto fail;

    n = th_wbrk(buffer, breaks, len);
    if (n == 0) {
        /* The entire buffer is one word. */
        result = PyList_New(1);
        if (!result)
            goto fail;

        Py_INCREF(text);
        PyList_SET_ITEM(result, 0, text);
        PyMem_Free(buffer);
        PyMem_Free(breaks);
        return result;
    }

    result = PyList_New(n + 1);
    if (!result)
        goto fail;

    for (s = 0, i = 0; i < n; i++) {
        tok = PyUnicode_FromWideChar(buffer+s, breaks[i]-s);
        if (!tok)
            goto fail;
        PyList_SET_ITEM(result, i, tok);
        s = breaks[i];
    }
    tok = PyUnicode_FromWideChar(buffer+s, len-s);
    if (!tok)
        goto fail;
    PyList_SET_ITEM(result, n, tok);

    PyMem_Free(breaks);
    PyMem_Free(buffer);
    return result;

 nomem:
    PyErr_NoMemory();
 fail:
    /* We do not hold a reference to 'text'. */
    Py_XDECREF(result);
    Py_XDECREF(tok);
    PyMem_Free(breaks);
    PyMem_Free(buffer);
    return NULL;
}

static PyMethodDef libthai_methods[] = {
    {"split", split_, METH_O,
     "Split text in the Thai language at word boundaries."},
    {NULL, NULL, 0, NULL}   /* sentinel */
};

static PyModuleDef libthai_module = {
    PyModuleDef_HEAD_INIT,
    "libthai",
    "Word breaking for Thai",
    0,
    libthai_methods,
    NULL, NULL, NULL, NULL
};

PyMODINIT_FUNC
PyInit_libthai(void)
{
    return PyModule_Create(&libthai_module);
}
