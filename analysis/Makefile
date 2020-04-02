# Most of the work is in the subdirectories.
# One C++ program lives in this directory.

SUBDIRS = cld2 html_extractor word_seg/dongdu word_seg/pythai

CXXFLAGS = -std=gnu++11 -g -O2 -Wall -Wextra

all: $(SUBDIRS) extract-from-iclab-json
clean: $(SUBDIRS:=-clean) .-clean

$(SUBDIRS): %:
	$(MAKE) -C $*

$(SUBDIRS:=-clean): %-clean:
	$(MAKE) -C $* clean

extract-from-iclab-json: extract-from-iclab-json.cc
	$(CXX) $(CXXFLAGS) -o $@ $<

.-clean:
	-rm -f extract-from-iclab-json

.PHONY: all clean $(SUBDIRS) $(SUBDIRS:=-clean) .-clean
