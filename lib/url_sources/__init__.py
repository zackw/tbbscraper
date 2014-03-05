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

def driver(argv):
    # Command-line-invokable source modules all have names starting "s_";
    # other modules in this package are shared code.  Load up all the
    # commands so we can access their argument parsers.

    commands = [(name[2:], finder.find_module(name).load_module(name))
                for finder, name, ispkg in pkgutil.iter_modules(__path__)
                if not ispkg and name.startswith("s_")]

    for c in commands:
        print(c[0], dir(c[1]))
