#
# Copyright (c) 2011 rPath, Inc.
#

from rpath_repeater.utils.base_forwarding_plugin import BaseException
from rpath_repeater.utils.base_forwarding_plugin import AuthenticationError  # pyflakes=ignore

class UpdateError(BaseException):
    pass

class NotEnoughSpaceError(BaseException):
    pass

class ServiceFailedToStartError(BaseException):
    pass
