NULL   =
CXX    = c++ -std=c++11
PYTHON = python3
CYTHON = cython3 -3

all: # is the default.
include python-vars.mk

all: cld2.$M

cld2.$M: cld2.$O
	$(CXX) $(LINKER_ARGS) -lcld2

clean:
	-rm -f \
	  cld2.$M cld2.$O cld2.cc \
	  $(CLD2_CORE_OBJS) \
	  python-vars.mk
	-rm -rf __pycache__

# Python boilerplate
python-vars.mk:
	$(PYTHON) ../get-module-compile-cmds.py $@

%.$O: %.cc
	$(CXX) $(COMPILER_ARGS)

%.cc: %.pyx
	$(CYTHON) --cplus -I. -o $@ $<

# Header dependencies
cld2.cc: cld2.pyx compact_lang_det.pxd

.PHONY: all clean
