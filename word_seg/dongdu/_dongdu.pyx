from os.path import dirname
from libcpp.string cimport string
from libcpp cimport bool

cdef extern from "Machine.h":
    enum: PREDICT

    cdef cppclass Machine:
        Machine(int, string, int) except +
        bool load()
        string segment(string)

__all__ = ('Segmenter',)

cdef class Segmenter:
    cdef Machine *machine

    def __init__(self):
        cdef bytes datadir = dirname(__file__).encode("utf-8")
        self.machine = new Machine(3, datadir, PREDICT)
        if not self.machine.load():
            raise RuntimeError("Failed to load Vietnamese segmentation data")

    def __dealloc__(self):
        del self.machine

    def segment(self, text):
        cdef bytes u8text
        if isinstance(text, unicode):
            u8text = text.encode('utf-8')
        elif isinstance(text, bytes):
            u8text = text
        else:
            raise TypeError("'text' argument must be a string")

        cdef bytes segmented = self.machine.segment(u8text)

        # dongdu produces a string with all intra-word spaces replaced
        # by underscores.  We want a list of normally-written words.
        return [s.replace("_", " ")
                for s in segmented.decode("utf-8").split()]
