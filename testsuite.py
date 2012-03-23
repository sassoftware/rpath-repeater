#!/usr/bin/python
# -*- mode: python -*-
#
# Copyright (c) 2010 rPath, Inc.  All Rights Reserved.
#

import sys

from testrunner import suite, testhandler

class Suite(suite.TestSuite):
    # Boilerplate. We need these values saved in the caller module
    testsuite_module = sys.modules[__name__]
    suiteClass = testhandler.ConaryTestSuite

    execPathVarNames = [
        'SMARTFORM_PATH',
        'XOBJ_PATH',
    ]

_s = Suite()
setup = _s.setup
main = _s.main

if __name__ == '__main__':
    _s.run()
