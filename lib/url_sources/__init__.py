# URL sources - common code and driver.
# Copyright © 2014 Zack Weinberg
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

import argparse
import os.path
import pkgutil

print(__name__)
print(__path__)

# Command-line-invokable source modules all have names starting "s_";
# other modules in this package are shared code.  Load up all the
# commands so we can access their argument parsers.

commands = [(n[2:], f.find_module(n).load_module(n))
            for f, n, _ in pkgutil.iter_modules(__path__)
            if n.startswith("s_")]

def driver(argv):
    print("got here: " + " ".join(argv))
    for c in commands:
        print(repr(c))
