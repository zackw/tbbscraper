NULL   =
CC     = cc -std=c11
PYTHON = python3
CYTHON = cython3 -3

all: # is the default.
include python-vars.mk

all: _extractor.$M grapheme_counter.$M relative_urls.$M

_extractor.$M: _extractor.$O
	$(CC) $(LINKER_ARGS)
grapheme_counter.$M: grapheme_counter.$O gbp_tbl.$O
	$(CC) $(LINKER_ARGS)
relative_urls.$M: relative_urls.$O
	$(CC) $(LINKER_ARGS)
boilerplate_removal.$M: boilerplate_removal.$O
	$(CC) $(LINKER_ARGS)

# gbp_tbl.* are generated from GraphemeBreakProperty.txt.
# The % signs tell Make that one invocation creates all three.
gbp_tbl.c% gbp_tbl.h% gbp_tbl.pxd%: gbp_tblgen.py GraphemeBreakProperty.txt
	$(PYTHON) gbp_tblgen.py

clean:
	-rm -f \
	   _extractor.$M _extractor.$O _extractor.c \
	   grapheme_counter.$M grapheme_counter.$O grapheme_counter.c \
	   relative_urls.$M relative_urls.$O relative_urls.c \
	   boilerplate_removal.$M boilerplate_removal.$O boilerplate_removal.c \
	   gbp_tbl.$O gbp_tbl.c gbp_tbl.h gbp_tbl.pxd \
	   python-vars.mk

# Header dependencies
_extractor.c: _extractor.pyx gumbo.pxd relative_urls.pxd
boilerplate_removal.c: boilerplate_removal.pyx gumbo.pxd grapheme_counter.pxd
grapheme_counter.c: grapheme_counter.pyx grapheme_counter.pxd gbp_tbl.pxd
relative_urls.c: relative_urls.pyx relative_urls.pxd

_extractor.$O: _extractor.c
gbp_tbl.$O: gbp_tbl.c gbp_tbl.h
grapheme_counter.$O: grapheme_counter.c gbp_tbl.h
relative_urls.$O: relative_urls.c

# Python boilerplate
python-vars.mk:
	$(PYTHON) ../get-module-compile-cmds.py $@

%.$O: %.c
	$(CC) $(COMPILER_ARGS)

%.c: %.pyx
	$(CYTHON) -I. -o $@ $<

.PHONY: all clean
