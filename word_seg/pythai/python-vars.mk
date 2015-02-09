O             = o
M             = cpython-34m.so
COMPILER_ARGS = -DNDEBUG -g -fwrapv -O2 -Wall -Wstrict-prototypes -g -fstack-protector-strong -Wformat -Werror=format-security -D_FORTIFY_SOURCE=2 -fPIC -I/usr/include/python3.4m -c $< -o $@
LINKER_ARGS   = -pthread -shared -Wl,-O1 -Wl,-Bsymbolic-functions -Wl,-z,relro -Wl,-z,relro -g -fstack-protector-strong -Wformat -Werror=format-security -D_FORTIFY_SOURCE=2 $^ $(LIBS) -o $@

python-vars.mk: ../../get-module-compile-cmds.py \
	/usr/lib/python3.4/distutils/__init__.py \
	/usr/lib/python3.4/distutils/archive_util.py \
	/usr/lib/python3.4/distutils/ccompiler.py \
	/usr/lib/python3.4/distutils/cmd.py \
	/usr/lib/python3.4/distutils/command/__init__.py \
	/usr/lib/python3.4/distutils/command/build.py \
	/usr/lib/python3.4/distutils/command/build_ext.py \
	/usr/lib/python3.4/distutils/config.py \
	/usr/lib/python3.4/distutils/core.py \
	/usr/lib/python3.4/distutils/debug.py \
	/usr/lib/python3.4/distutils/dep_util.py \
	/usr/lib/python3.4/distutils/dir_util.py \
	/usr/lib/python3.4/distutils/dist.py \
	/usr/lib/python3.4/distutils/errors.py \
	/usr/lib/python3.4/distutils/extension.py \
	/usr/lib/python3.4/distutils/fancy_getopt.py \
	/usr/lib/python3.4/distutils/file_util.py \
	/usr/lib/python3.4/distutils/log.py \
	/usr/lib/python3.4/distutils/spawn.py \
	/usr/lib/python3.4/distutils/sysconfig.py \
	/usr/lib/python3.4/distutils/unixccompiler.py \
	/usr/lib/python3.4/distutils/util.py
