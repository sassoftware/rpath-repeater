#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import time
import logging

from conary.conaryclient import callbacks

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

    def start(self, msg):
        pass

    def done(self, msg):
        pass


class ThrottleCallback(object):
    def __init__(self, logFunc):
        self._logFunc = logFunc

        self._last = None
        self._last_t = 0

    def __call__(self, msg):
        if (self._logFunc is not None and msg != self._last and
            (time.time() - self._last_t > 1)):
            self._last = msg
            self._last_t = time.time()
            self._logFunc(msg)


class FileCopyCallback(object):
    def __init__(self, msg, total, cbfn):
        self.msg = msg
        self.total = total
        self.cbfn = ThrottleCallback(cbfn)

    def __call__(self, amount, rate):
        # units are bytes and bytes/second, convert to KB
        total = self.total / 1024.0
        amount = amount / 1024.0
        rate = rate / 1024
        msg = ' %d/%dKB at %dKB/s' % (amount, total, rate)
        self.cbfn(self.msg + msg)


class ChangeSetCallback(callbacks.ChangesetCallback):
    def __init__(self, *args, **kwargs):
        self._message = ThrottleCallback(kwargs.pop('logFunc', None))
        callbacks.ChangesetCallback.__init__(self, *args, **kwargs)

    def __del__(self):
        pass


class RepeaterWMICallback(WMICallback, BaseCallback):
    def __init__(self, authInfo, statusMethod):
        WMICallback.__init__(self, authInfo)

        self._statusMethod = statusMethod
        self._logger = logging.getLogger('rpath_repeater.utils.windows')

    def _log(self, level, msg=None):
        if not msg:
            msg = ''
        msg = self._prependHost(msg)
        self._statusMethod(level, msg)

    def info(self, msg):
        self._log(Codes.MSG_GENERIC, msg)
        self._logger.info(msg)

    def error(self, msg):
        self._log(Codes.ERR_GENERIC, msg)
        self._logger.error(msg)

    def debug(self, msg):
        msg = self._prependHost(msg)
        self._logger.debug(msg)

    setStatus = debug

    def copyfile(self, msg, size):
        return FileCopyCallback(msg, size, self.info)

    def getChangeSetCallback(self):
        return ChangeSetCallback(logFunc=self.info)

    def start(self, msg=None):
        self._log(Codes.MSG_START, msg)

    def done(self, msg=None):
        self._log(Codes.OK, msg)
