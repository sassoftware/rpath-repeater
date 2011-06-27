#
# Copyright (c) 2011 rPath, Inc.
#

from rpath_repeater.utils.base_forwarding_plugin import BaseException

class UpdateError(BaseException):
    pass

class NotEnoughSpaceError(BaseException):
    pass
