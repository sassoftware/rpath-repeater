#!/usr/bin/python
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

import pywbem
import wbemlib
import cimjobhandler

WBEMException = wbemlib.WBEMException

class CIMUpdater(cimjobhandler.CIMJobHandler):
    '''
    Class for checking and applying updates to a remote appliance via CIM.
    Exposes both asynchronous and synchronous methods to check for and apply
    updates.
    '''

    def __init__(self, server, logger=None):
        super(CIMUpdater, self).__init__(server, logger=logger)
        self._updateCheckReturnValues = None
        self._elementSoftwareStatusValues = None

    def _getSoftwareElementStatusValues(self, force = False):
        if not self._elementSoftwareStatusValues or force:
            cimClass = self.server.VAMI_ElementSoftwareIdentity.GetClass()
            prop = cimClass.properties['ElementSoftwareStatus']
            states = prop.qualifiers
            self._elementSoftwareStatusValues = self._normalizeValueMap(
                states['Values'].value, states['ValueMap'].value,
                prop.type)
        return self._elementSoftwareStatusValues
    elementSoftwareStatusValues = property(_getSoftwareElementStatusValues)

    def getInstalledItemList(self):
        # Select the ones that have Installed and Available as
        # ElementSoftwareStatus. See the mof for the value mappings
        return self._filterItemList([2, 6])

    def getAvailableItemList(self):
        return self._filterItemList([8])

    def getInstalledGroups(self):
        # XXX this is fairly low-level, we should probably try to wrap some of
        # these in wbemlib
        installedGroups = self.getInstalledItemList()
        ids = [ g['Antecedent']['InstanceID'] for g in installedGroups ]
        instanceNames = [ wbemlib.pywbem.cim_obj.CIMInstanceName(
            'VAMI_SoftwareIdentity', keybindings = dict(InstanceId = i))
            for i in ids ]
        instances = [ self.server.VAMI_SoftwareIdentity.GetInstance(i)
            for i in instanceNames ]
        ret = [ "%s=%s" % (x['name'], x['VersionString'])
            for x in instances ]
        return ret

    def _filterItemList(self, states):
        insts = self.server.VAMI_ElementSoftwareIdentity.EnumerateInstances()
        targetState = set(states)
        insts = [ x for x in insts
            if targetState.issubset(x.properties['ElementSoftwareStatus'].value)]
        return insts

    def updateCheckAsync(self):
        result = self.server.VAMI_SoftwareInstallationService.CheckAvailableUpdates()

        if result[0] != 4096L:
            self._unexpectedReturnCode('VAMI_SoftwareInstallationService', 
                'CheckAvailableUpdates', result[0], 4096L)

        job = result[1]['job']
        return job

    def updateCheck(self, timeout=None):
        job = self.updateCheckAsync()
        return self.handleJob(job, timeout = timeout)

    def applyUpdateAsync(self, sources, test, nodes):
        opts = [pywbem.Uint16(2)] # Migrate.
        if test:
            opts.append(pywbem.Uint16(4))
        return self.callMethodAsync('VAMI_SoftwareInstallationService',
            'InstallFromNetworkLocations',
            methodKwargs=dict(
                ManagementNodeAddresses=nodes,
                Sources=sources,
                InstallOptions=opts))

    def applyUpdate(self, sources, test, timeout = None, nodes=None):
        job = self.applyUpdateAsync(sources, test, nodes)
        return self.handleJob(job, timeout = timeout)

    def checkAndApplyUpdate(self, timeout = None):
        job = self.updateCheck(timeout = timeout)
        if job is None:
            return
        if not self.isJobSuccessful(job):
            error = self.server.getError(job)
            self.log_error(error)
            raise RuntimeError("Error checking for available software")
        job = self.applyUpdate(timeout = timeout)
        if not self.isJobSuccessful(job):
            error = self.server.getError(job)
            self.log_error(error)
            raise RuntimeError('Error while applying updates. The error from '
                'the managed system was: %s' % error)

    def log_error(self, error):
        if self.logger:
            self.logger.error(error)

if __name__ == '__main__':
    host = 'https://ec2-174-129-153-120.compute-1.amazonaws.com'
    x509 = dict(cert_file = "/tmp/cert.crt", key_file = "/tmp/cert.key")
    updater = CIMUpdater(host, x509 = x509)
    updater.checkAndApplyUpdate()
