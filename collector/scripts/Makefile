# This Makefile compiles the scripts that are written in C.  They're
# written in C because several of them have to be setuid, so we use
# aggressive warnings and hardening settings.

ALL = isolate openvpn-netns tunnel-ns neterr-details

CPPFLAGS = -D_FORTIFY_SOURCE=2
CFLAGS   = -std=c11 -g -O2 -pie -fPIE -fwhole-program -fstack-protector-strong \
           -Werror -Wall -Wextra -Wpedantic -Wformat=2 -Wstrict-overflow \
           -Wundef -Wshadow -Wwrite-strings \
           -Wconversion -Wlogical-op -Wstrict-prototypes -Wstack-protector
LDFLAGS  = -Wl,-z,relro,-z,now

all: $(ALL)
clean:
	-rm -f $(ALL)

$(ALL): %: %.c
	$(CC) $< -o $@ $(CFLAGS) $(CPPFLAGS) $(LDFLAGS) $(LIBS)

neterr-details: LIBS = -lgnutls

.PHONY: all clean
