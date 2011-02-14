#
# Copyright (c) 2011 rPath, Inc.
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

from rmake3.core import plug_dispatcher
from rmake3.core import handler
from rpath_repeater.codes import Codes as C
from rpath_repeater.utils import base_forwarding_plugin as bfp

IMAGE_UPLOAD_JOB = bfp.PREFIX + '.imageuploadplugin'

class ImageUploadPlugin(plug_dispatcher.DispatcherPlugin):
    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(ImageUploadHandler)

class ImageUploadHandler(handler.JobHandler):
    jobType = IMAGE_UPLOAD_JOB

    def starting(self):
        self.params = self.getData().thaw().getDict()
        putFilesURL = self.params['putFilesURL']
        statusReportURL = self.params['statusReportURL']
        image = self.params['image']
        bfp.ImageUpload(image, statusReportURL, putFilesURL)

        self.setStatus(C.OK, "Image import initiated")
