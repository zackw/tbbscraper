# URL sources - common code and driver.
# Copyright © 2014 Zack Weinberg
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

import argparse
import pkgutil

def driver():
    # Command-line-invokable source modules all have names starting "s_";
    # other modules in this package are shared code.  Load up all the
    # commands so we can access their argument parsers.

    commands = [(name[2:], finder.find_module(name).load_module(name))
                for finder, name, ispkg in pkgutil.iter_modules(__path__)
                if not ispkg and name.startswith("s_")]

    ap = argparse.ArgumentParser(
        usage="%(prog)s command ...",
        description="""
        Manage databases of URLs from various sources.
        For help on specific commands, use %(prog)s COMMAND --help.""",
        add_help=False)

    # Better handling of foo -h vs. foo CMD -h.
    ap.add_argument("-h", "--help", action="help", help=argparse.SUPPRESS)

    # The 'func' trick recommended in the argparse manual does not
    # appear to work.  Use 'dest' and a dictionary on the side instead.
    cps = ap.add_subparsers(title="commands", prog=ap.prog, metavar="",
                            dest="command")
    command_runners = {}
    for c in commands:
        try:
            name   = c[0]
            mod    = c[1]
            desc   = mod.__doc__
            setup  = mod.setup_argp
            run    = mod.run
        except AttributeError:
            # Skip any module that is missing at least one of the
            # required attributes.
            continue

        command_runners[name] = run
        cp = cps.add_parser(name,
                            help=desc.partition("\n\n")[0],
                            description=desc)

        # The -d option is common to all commands, but we cannot just
        # add it to the parent parser, because that would be too simple.
        # (If you try that, the -d option has to come before the command,
        # and is only mentioned for --help with no command...)
        cp.add_argument("-d", "--database", metavar="DB",
                        help="The database to update.",
                        default="urls.db")

        setup(cp)

    args = ap.parse_args()
    # If no command was specified, args.command will be None.
    # There does not appear to be any way to change this.
    if args.command is None:
        ap.print_help()
        raise SystemExit(2)

    command_runners[args.command](args)
