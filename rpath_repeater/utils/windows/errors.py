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
