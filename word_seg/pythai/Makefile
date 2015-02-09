NULL   =
CC     = cc -std=c11
PYTHON = python3

all: # is the default.
include python-vars.mk

all: libthai.$M

libthai.$M: LIBS = -lthai
libthai.$M: libthai.$O
	$(CC) $(LINKER_ARGS)

clean:
	-rm -f \
	  libthai.$M libthai.$O \
	  python-vars.mk
	-rm -rf __pycache__

# Python boilerplate
python-vars.mk:
	$(PYTHON) ../../get-module-compile-cmds.py $@

%.$O: %.c
	$(CC) $(COMPILER_ARGS)

# Header dependencies
# libthai.c only depends on system headers
libthai.$O: libthai.c
