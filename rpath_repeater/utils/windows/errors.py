#
# Copyright (c) 2011 rPath, Inc.
#

from rpath_repeater.utils.base_forwarding_plugin import WmiError
from rpath_repeater.utils.base_forwarding_plugin import GenericError
from rpath_repeater.utils.base_forwarding_plugin import BaseException
from rpath_repeater.utils.base_forwarding_plugin import CIFSMountError
from rpath_repeater.utils.base_forwarding_plugin import AuthenticationError
from rpath_repeater.utils.base_forwarding_plugin import RegistryAccessError
from rpath_repeater.utils.base_forwarding_plugin import WindowsServiceError

class UpdateError(BaseException):
    pass

class NotEnoughSpaceError(BaseException):
    pass
