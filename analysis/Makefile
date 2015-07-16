# All the work is in the subdirectories.

SUBDIRS = cld2 html_extractor word_seg/mmseg word_seg/dongdu word_seg/pythai

all: $(SUBDIRS)
clean: $(SUBDIRS:=-clean)

$(SUBDIRS): %:
	$(MAKE) -C $*

$(SUBDIRS:=-clean): %-clean:
	$(MAKE) -C $* clean

.PHONY: all clean $(SUBDIRS) $(SUBDIRS:=-clean)
