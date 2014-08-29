# This Makefile compiles the scripts that are written in C.
# They're written in C because they have to be setuid, so we
# use aggressive warnings and hardening settings.  Currently
# hardwired for GCC 4.9.

ALL = isolate openvpn-netns

CPPFLAGS = -D_FORTIFY_SOURCE=2
CFLAGS   = -std=c11 -g -O2 -pie -fPIE -fwhole-program -fstack-protector-strong \
           -Werror -Wall -Wextra -Wpedantic -Wformat=2 -Wstrict-overflow \
           -Wundef -Wshadow -Wbad-function-cast -Wwrite-strings \
           -Wconversion -Wlogical-op -Wstrict-prototypes -Wstack-protector
LDFLAGS  = -Wl,-z,relro,-z,now

# Address sanitizer disabled by default because it chews up so much
# address space that it runs into RLIMIT_AS restrictions!
#CFLAGS += -fsanitize=address
CFLAGS  += -fsanitize=undefined

all: $(ALL)
clean:
	-rm -f $(ALL)

$(ALL): %: %.c
	$(CC) $< -o $@ $(CFLAGS) $(CPPFLAGS) $(LDFLAGS)

.PHONY: all clean
