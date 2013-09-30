INSTALL = install
DESTDIR =
prefix  = /usr

bindir  = $(prefix)/bin
libdir  = $(prefix)/lib/tor-browser
docdir  = $(prefix)/share/doc/tor-browser

all:
	./prime-tor-directory-cache

install:
	$(INSTALL) -d $(DESTDIR)$(bindir)
	$(INSTALL) start-tor-browser $(DESTDIR)$(prefix)/bin

	$(INSTALL) -d $(DESTDIR)$(libdir)
	cp -r --preserve=mode App  $(DESTDIR)$(libdir)/App
	cp -r --preserve=mode Lib  $(DESTDIR)$(libdir)/Lib
	cp -r --preserve=mode Data $(DESTDIR)$(libdir)/Data
	rm -rf $(DESTDIR)$(libdir)/Lib/libz/
	rm -f $(DESTDIR)$(libdir)/Lib/libcrypto.so*
	rm -f $(DESTDIR)$(libdir)/Lib/libssl.so*
	chmod -R go+rX $(DESTDIR)$(libdir)

	$(INSTALL) -d $(DESTDIR)$(docdir)
	$(INSTALL) -m 0644 Docs/* $(DESTDIR)$(docdir)
	$(INSTALL) -m 0644 sources/* $(DESTDIR)$(docdir)

.PHONY: all install
