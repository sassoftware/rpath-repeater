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

import logging

from rmake3.core import plug_dispatcher
from rmake3.core import handler
from rpath_repeater.models import ImageFile, URL
from rpath_repeater.codes import Codes as C
from rpath_repeater.utils import base_forwarding_plugin as bfp

log = logging.getLogger(__name__)

IMAGE_UPLOAD_JOB = bfp.PREFIX + '.imageuploadplugin'

class ImageUploadPlugin(plug_dispatcher.DispatcherPlugin):
    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(ImageUploadHandler)

class ImageUploadHandler(handler.JobHandler):
    jobType = IMAGE_UPLOAD_JOB

    def starting(self):
        self.params = self.getData().thaw().getDict()
        self.imageList = [ ImageFile(url=URL(**x['url']),
                destination=URL(**x['destination']),
                progress=URL(**x['progress']),
                headers=x.get('headers'))
            for x in self.params.get('imageList', []) ]
        for image in self.imageList:
            self.processImage(image)

        self.setStatus(C.OK, "Done")

    def processImage(self, image):
        splicer = bfp.Splicer(image.url, image.destination, image.progress)
        return splicer
