NULL   =
CXX    = c++ -std=c++11
PYTHON = python3
CYTHON = cython3 -3

all: # is the default.
include python-vars.mk

CLD2_CORE_OBJS = \
	compact_lang_det.$O \
	compact_lang_det_impl.$O \
	compact_lang_det_hint_code.$O \
	lang_script.$O \
	tote.$O \
	getonescriptspan.$O \
	scoreonescriptspan.$O \
	cldutil.$O \
	cldutil_shared.$O \
	utf8statetable.$O \
	generated_language.$O \
	generated_ulscript.$O \
	generated_distinct_bi_0.$O \
	cld_generated_cjk_delta_bi_32.$O \
	cld_generated_cjk_uni_prop_80.$O \
	cld_generated_score_quad_octa_0122.$O \
	cld2_generated_cjk_compatible.$O \
	cld2_generated_deltaocta0122.$O \
	cld2_generated_distinctocta0122.$O \
	cld2_generated_quad0122.$O \
	$(NULL)

all: cld2.$M

cld2.$M: cld2.$O $(CLD2_CORE_OBJS)
	$(CXX) $(LINKER_ARGS)

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
_basic_test_program.$O: _basic_test_program.cc compact_lang_det.h \
 lang_script.h generated_language.h generated_ulscript.h encodings.h
cld2.$O: cld2.cc compact_lang_det.h lang_script.h generated_language.h \
 generated_ulscript.h encodings.h
cld2_generated_cjk_compatible.$O: cld2_generated_cjk_compatible.cc \
 cld2tablesummary.h
cld2_generated_deltaocta0122.$O: cld2_generated_deltaocta0122.cc \
 cld2tablesummary.h
cld2_generated_distinctocta0122.$O: cld2_generated_distinctocta0122.cc \
 cld2tablesummary.h
cld2_generated_quad0122.$O: cld2_generated_quad0122.cc cld2tablesummary.h
cld_generated_cjk_delta_bi_32.$O: cld_generated_cjk_delta_bi_32.cc \
 cld2tablesummary.h
cld_generated_cjk_uni_prop_80.$O: cld_generated_cjk_uni_prop_80.cc \
 utf8statetable.h stringpiece.h
cld_generated_score_quad_octa_0122.$O: \
 cld_generated_score_quad_octa_0122.cc
cldutil.$O: cldutil.cc cldutil.h generated_language.h cldutil_shared.h \
 cld2tablesummary.h generated_ulscript.h lang_script.h \
 scoreonescriptspan.h langspan.h utf8statetable.h stringpiece.h tote.h
cldutil_shared.$O: cldutil_shared.cc cldutil_shared.h cld2tablesummary.h \
 unaligned_access.h utf8statetable.h stringpiece.h
compact_lang_det.$O: compact_lang_det.cc compact_lang_det.h lang_script.h \
 generated_language.h generated_ulscript.h encodings.h \
 compact_lang_det_impl.h
compact_lang_det_hint_code.$O: compact_lang_det_hint_code.cc \
 compact_lang_det_hint_code.h encodings.h generated_language.h \
 lang_script.h generated_ulscript.h unaligned_access.h
compact_lang_det_impl.$O: compact_lang_det_impl.cc \
 compact_lang_det_impl.h compact_lang_det.h lang_script.h \
 generated_language.h generated_ulscript.h encodings.h cld2tablesummary.h \
 cldutil.h compact_lang_det_hint_code.h getonescriptspan.h langspan.h \
 scoreonescriptspan.h utf8statetable.h stringpiece.h tote.h
generated_distinct_bi_0.$O: generated_distinct_bi_0.cc cld2tablesummary.h
generated_language.$O: generated_language.cc generated_language.h \
 generated_ulscript.h
generated_ulscript.$O: generated_ulscript.cc generated_ulscript.h \
 generated_language.h
getonescriptspan.$O: getonescriptspan.cc getonescriptspan.h langspan.h \
 generated_language.h generated_ulscript.h stringpiece.h \
 unaligned_access.h utf8prop_lettermarkscriptnum.h utf8statetable.h \
 utf8repl_lettermarklower.h utf8scannot_lettermarkspecial.h
lang_script.$O: lang_script.cc lang_script.h generated_language.h \
 generated_ulscript.h
scoreonescriptspan.$O: scoreonescriptspan.cc scoreonescriptspan.h \
 cld2tablesummary.h generated_language.h generated_ulscript.h langspan.h \
 utf8statetable.h stringpiece.h cldutil.h lang_script.h tote.h
tote.$O: tote.cc tote.h
utf8statetable.$O: utf8statetable.cc utf8statetable.h stringpiece.h \
 unaligned_access.h

.PHONY: all clean
