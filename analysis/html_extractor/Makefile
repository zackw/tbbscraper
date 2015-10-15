NULL   =
AWK    = awk
CC     = cc -std=c11
PYTHON = python3
CYTHON = cython3 -3

all: # is the default.
include python-vars.mk

all: _extractor.$M unicode_utils.$M relative_urls.$M boilerplate_removal.$M

_extractor.$M: LIBS = -lgumbo
_extractor.$M: _extractor.$O prescan.$O
	$(CC) $(LINKER_ARGS)
unicode_utils.$M: unicode_utils.$O gbp_tbl.$O
	$(CC) $(LINKER_ARGS)
relative_urls.$M: relative_urls.$O
	$(CC) $(LINKER_ARGS)
boilerplate_removal.$M: LIBS = -lgumbo
boilerplate_removal.$M: boilerplate_removal.$O
	$(CC) $(LINKER_ARGS)

# gbp_tbl.* are generated from GraphemeBreakProperty.txt.  Writing this
# as a pattern rule tells Make that one invocation creates all three.
%_tbl.c %_tbl.h %_tbl.pxd: %_tblgen.py GraphemeBreakProperty.txt
	$(PYTHON) $<

uncanon-max.inc: encodings.inc uncanon-max.awk
	$(AWK) -f uncanon-max.awk encodings.inc > uncanon-max.incT
	mv -f uncanon-max.incT uncanon-max.inc

clean:
	-rm -f \
	   _extractor.$M _extractor.$O _extractor.c \
	   unicode_utils.$M unicode_utils.$O unicode_utils.c \
	   relative_urls.$M relative_urls.$O relative_urls.c \
	   boilerplate_removal.$M boilerplate_removal.$O boilerplate_removal.c \
	   gbp_tbl.$O gbp_tbl.c gbp_tbl.h gbp_tbl.pxd \
	   uncanon-max.inc uncanon-max.incT \
	   python-vars.mk
	-rm -rf __pycache__

# Header dependencies
_extractor.c: _extractor.pyx gumbo.pxd prescan.pxd relative_urls.pxd \
  boilerplate_removal.pxd unicode_utils.pxd
boilerplate_removal.c: boilerplate_removal.pyx gumbo.pxd unicode_utils.pxd
unicode_utils.c: unicode_utils.pyx unicode_utils.pxd gbp_tbl.pxd
relative_urls.c: relative_urls.pyx relative_urls.pxd

_extractor.$O: _extractor.c prescan.h
prescan.$O: prescan.c prescan.h encodings.inc uncanon-max.inc
gbp_tbl.$O: gbp_tbl.c gbp_tbl.h
unicode_utils.$O: unicode_utils.c gbp_tbl.h
relative_urls.$O: relative_urls.c

# Python boilerplate
python-vars.mk:
	$(PYTHON) ../get-module-compile-cmds.py $@

%.$O: %.c
	$(CC) $(COMPILER_ARGS)

%.c: %.pyx
	$(CYTHON) -I. -o $@ $<

.PHONY: all clean
