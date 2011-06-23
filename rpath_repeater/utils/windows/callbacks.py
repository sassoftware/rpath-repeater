#
# Copyright (c) 2011 rPath, Inc.
#

import logging

from wmiclient import WMICallback
from rpath_repeater.codes import Codes

class BaseCallback(object):
    def __init__(self, *args, **kwargs):
        pass

    def info(self, msg):
        pass

    def setStatus(self, msg):
        pass

    def error(self, msg):
        pass

    def debug(self, msg):
        pass

    def copyfile(self, name, version, flavor):
        pass


class FileCopyCallback(object):
    def __init__(self, msg, total, cbfn):
        self.msg = msg
        self.total = total
        self.cbfn = cbfn

    def __call__(self, amount, rate):
        # units are bytes and bytes/second, convert to KB
        total = self.total / 1024.0
        amount = amount / 1024.0
        rate = rate / 1024
        msg = ' %d/%dKB at %dKB/s' % (amount, total, rate)
        self.cbfn(self.msg + msg)


class RepeaterWMICallback(WMICallback, BaseCallback):
    def __init__(self, authInfo, statusMethod):
        WMICallback.__init__(self, authInfo)

        self._statusMethod = statusMethod
        self._logger = logging.getLogger('rpath_repeater.utils.windows')

    def _log(self, level, msg):
        msg = self._prependHost(msg)
        self._statusMethod(level, msg)

    def info(self, msg):
        self._log(Codes.MSG_GENERIC, msg)

    def error(self, msg):
        self._log(Codes.ERR_GENERIC, msg)

    def debug(self, msg):
        msg = self._prependHost(msg)
        self._logger.debug(msg)

    setStatus = debug

    def copyfile(self, msg, size):
        return FileCopyCallback(msg, size, self.info)
