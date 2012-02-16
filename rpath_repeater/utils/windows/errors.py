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
    error = 'The rPath Tools Installer Service failed to start.'

class MSIInstallationError(BaseException):
    error = ('The rPathTools MSI failed to install. Please check the '
        'installation log in C:\Windows\Temp\rpath_install_<date>.log for more '
        'information.')
