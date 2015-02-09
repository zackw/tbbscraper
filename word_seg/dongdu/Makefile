NULL   =
CC     = cc -std=c11
CXX    = c++ -std=c++11
PYTHON = python3
CYTHON = cython3 -3

all: # is the default.
include python-vars.mk

all: _dongdu.$M

_dongdu.$M: _dongdu.$O Machine.$O Feats.$O StrMap.$O DicMap.$O SylMap.$O \
            linear.$O tron.$O dnrm2.$O ddot.$O daxpy.$O dscal.$O
	$(CXX) $(LINKER_ARGS)

clean:
	-rm -f _dongdu.$M \
	  _dongdu.$O Machine.$O Feats.$O StrMap.$O DicMap.$O SylMap.$O \
          linear.$O tron.$O dnrm2.$O ddot.$O daxpy.$O dscal.$O \
	  _dongdu.cpp \
	  python-vars.mk
	-rm -rf __pycache__

# Python boilerplate
python-vars.mk:
	$(PYTHON) ../../get-module-compile-cmds.py $@

%.$O: %.cpp
	$(CXX) $(COMPILER_ARGS)

%.$O: %.c
	$(CC) $(COMPILER_ARGS)

%.cpp: %.pyx
	$(CYTHON) --cplus -I. -o $@ $<

# Header dependencies
_dongdu.$O: _dongdu.cpp Machine.h Feats.h configure.h SylMap.h DicMap.h \
 StrMap.h linear.h
Machine.$O: Machine.cpp Machine.h Feats.h configure.h SylMap.h DicMap.h \
 StrMap.h linear.h
Feats.$O: Feats.cpp Feats.h configure.h SylMap.h
SylMap.$O: SylMap.cpp SylMap.h
DicMap.$O: DicMap.cpp DicMap.h
StrMap.$O: StrMap.cpp StrMap.h configure.h
linear.$O: linear.cpp linear.h tron.h
tron.$O: tron.cpp tron.h blas.h blasp.h
daxpy.$O: daxpy.c blas.h blasp.h
ddot.$O: ddot.c blas.h blasp.h
dnrm2.$O: dnrm2.c blas.h blasp.h
dscal.$O: dscal.c blas.h blasp.h
