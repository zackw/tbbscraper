NULL =
CXXFLAGS = -std=c++11 -g -O2 -W -Wall

OBJECTS = \
	_basic_test_program.o \
	compact_lang_det.o \
	compact_lang_det_impl.o \
	compact_lang_det_hint_code.o \
	lang_script.o \
	tote.o \
	getonescriptspan.o \
	scoreonescriptspan.o \
	cldutil.o \
	cldutil_shared.o \
	utf8statetable.o \
	generated_language.o \
	generated_ulscript.o \
	generated_distinct_bi_0.o \
	cld_generated_cjk_delta_bi_32.o \
	cld_generated_cjk_uni_prop_80.o \
	cld_generated_score_quad_octa_0122.o \
	cld2_generated_cjk_compatible.o \
	cld2_generated_deltaocta0122.o \
	cld2_generated_distinctocta0122.o \
	cld2_generated_quad0122.o \
	$(NULL)

all: _basic_test_program

_basic_test_program: $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	-rm -f $(OBJECTS) _basic_test_program
