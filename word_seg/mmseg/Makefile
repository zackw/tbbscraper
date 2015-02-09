NULL   =
CXX    = c++ -std=c++11
PYTHON = python3

all: # is the default.
include python-vars.mk

all: _mmseg.$M

_mmseg.$M: mmseg.$O algor.$O dict.$O memory.$O
	$(CXX) $(LINKER_ARGS)

clean:
	-rm -f \
	  _mmseg.$M mmseg.$O algor.$O dict.$O memory.$O \
	  python-vars.mk
	-rm -rf __pycache__

# Python boilerplate
python-vars.mk:
	$(PYTHON) ../../get-module-compile-cmds.py $@

%.$O: %.cpp
	$(CXX) $(COMPILER_ARGS)

# Header dependencies
algor.$O: algor.cpp rules.h chunk.h word.h memory.h algor.h token.h dict.h
dict.$O: dict.cpp dict.h word.h memory.h
memory.$O: memory.cpp memory.h
mmseg.$O: mmseg.cpp utils.h token.h dict.h word.h memory.h algor.h chunk.h
