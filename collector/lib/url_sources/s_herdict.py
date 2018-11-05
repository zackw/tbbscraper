# Copyright © 2013, 2014, 2017 Zack Weinberg
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# There is NO WARRANTY.

"""Extract URLs logged as inaccessible by herdict.org."""

def setup_argp(ap):
    pass

def run(args):
    from shared.monitor import Monitor
    from url_sources.herdict import HerdictExtractor

    ext = HerdictExtractor(args)
    Monitor(ext, banner="Extracting URLs from herdict.org")
    ext.report_final_statistics()

