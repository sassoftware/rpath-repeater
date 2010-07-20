#
# Copyright (c) 2010 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import logging
import sys

format = '[%(asctime)s]%(levelname)-8s"%(message)s"','%Y-%m-%d %a %H:%M:%S'

def getlogger():
    logger = logging.getLogger()
    hdlr = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(format)

    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.NOTSET)

    return logger

def debug(msg):
    logger = getlogger()
    logger.debug(msg)

def error(msg):
    logger = getlogger()
    logger.error(msg)

def exception(msg):
    logger = getlogger()
    logger.exception(msg)

def logFailure(failure, msg='Unhandled exception in deferred:'):
    """Log a Twisted Failure object with traceback.

    Suitable for use as an errback.
    """
    logging.error('%s\n%s' , msg, failure.getTraceback())