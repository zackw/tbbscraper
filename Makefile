# All the work is in the subdirectories.

SUBDIRS = cld2 html_extractor

all: $(SUBDIRS)
clean: $(SUBDIRS:=-clean)

$(SUBDIRS): %:
	$(MAKE) -C $*

$(SUBDIRS:=-clean): %-clean:
	$(MAKE) -C $* clean

.PHONY: all clean $(SUBDIRS) $(SUBDIRS:=-clean)
