#!/usr/bin/python
#
# Copyright (c) 2012 rPath, Inc.
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

import wbemlib
import cimjobhandler

WBEMException = wbemlib.WBEMException

class CIMSurveyScanner(cimjobhandler.CIMJobHandler):
    '''
    Class for checking and applying updates to a remote appliance via CIM.
    Exposes both asynchronous and synchronous methods to check for and apply
    updates.
    '''


    def scanAsync(self, desiredTopLevelItems):
        result = self.server.RPATH_SystemSurveyService.Scan(desiredTopLevelItems)

        if result[0] != 4096L:
            self._unexpectedReturnCode('RPATH_SystemSurveyService',
                'Scan', result[0], 4096L)

        job = result[1]['job']
        return job

    def scan(self, desiredTopLevelItems, timeout=None):
        job = self.scanAsync(desiredTopLevelItems)
        return self.pollJobForCompletion(job, timeout=timeout)
