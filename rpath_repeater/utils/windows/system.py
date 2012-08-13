#
# Copyright (c) 2011 rPath, Inc.
#

"""
Representation of a Windows system.
"""

from rmake3.lib import uuid

from wmiclient import WMIClient
from wmiclient import WMIAccessDeniedError

from rpath_repeater.utils.windows import errors
from rpath_repeater.utils.windows.rtis import rTIS
from rpath_repeater.utils.windows.updates import UpdateJob
from rpath_repeater.utils.windows.inventory import Inventory
from rpath_repeater.utils.windows.smbclient import SMBClient
from rpath_repeater.utils.windows.errors import AuthenticationError
from rpath_repeater.utils.windows.callbacks import RepeaterWMICallback

def cleanup(func):
    def wrapper(self, *args, **kwargs):
        try:
            res = func(self, *args, **kwargs)
        except WMIAccessDeniedError, e:
            raise AuthenticationError, str(e)
        finally:
            self.wmi.close()
            self.smb.close()
        return res
    return wrapper


class WindowsSystem(object):
    """
    Class for interacting with a remote Windows system.
    """

    def __init__(self, authInfo, setStatusMethod):
        self.callback = RepeaterWMICallback(authInfo, setStatusMethod)
        self.wmi = WMIClient(authInfo, callback=self.callback)
        self.smb = SMBClient(authInfo, callback=self.callback)

        self.rtis = rTIS(self.wmi, self.smb, callback=self.callback)
        self.inventory = Inventory(self.wmi, callback=self.callback)

        try:
            self.rtis.setup()
            self.inventory.setup()
        except WMIAccessDeniedError, e:
            raise AuthenticationError, str(e)

    @cleanup
    def register(self):
        self.callback.info('Registering System')

        # Generate UUIDs
        if not self.inventory.generatedUUID:
            self.inventory.generatedUUID = str(uuid.uuid4())

        result, localUUID = self.wmi.queryUUID()
        self.inventory.localUUID = localUUID

        self.callback.info('Registration Complete')

        return (localUUID, self.inventory.generatedUUID,
            self.inventory.computerName)

    @cleanup
    def poll(self):
        self.callback.info('Polling System')
        uuids = self.inventory.uuids
        hostname = self.inventory.computerName
        softwareVersions = self.rtis.polling_manifest
        netInfo = self.inventory.networkInfo
        self.callback.info('Polling Complete')
        return uuids, hostname, softwareVersions, netInfo

    @cleanup
    def shutdown(self):
        self.callback.error('Shutdown is not support for managed '
            'Windows systems')
        raise NotImplementedError

    def _getUpdateJob(self, jobId):
        return UpdateJob(self.rtis.flavor, self.rtis.manifest,
            self.rtis.system_model, jobId, callback=self.callback)

    @cleanup
    def update(self, troveSpecs, jobId, test=False):
        self.callback.info('Updating System')

        # Wait for the service to become available.
        self.rtis.wait(allowReboot=False, firstRun=True)

        self.callback.info('Retrieving installed software')

        updJob = self._getUpdateJob(jobId)
        updJob.prepareUpdate(troveSpecs, test=test)

        # Have to calculate the preview early so that in the case of initial
        # install of rPathTools, rPathTools will be included in the preview.
        preview = updJob.toxml()

        if test:
            return None, preview

        if not self.rtis.isInstalled:
            if 'rPathTools:msi' not in updJob:
                error = ('rPathTools:msi is not available for installation, '
                    'can not continue.')
                self.callback.error(error)
                raise errors.UpdateError, error

            self.rtis.applyCriticalUpdate(updJob)

        results = self.rtis.applyUpdate(updJob)

        self.callback.info('Update Complete')
        return results, preview

    @cleanup
    def configure(self, values, jobId):
        self.callback.info('Configuring System')

        self.rtis.wait(allowReboot=False, firstRun=True)

        results = self.rtis.applyConfiguration(values, jobId)

        self.callback.info('Configuration Complete')

        return results

    @cleanup
    def scan(self, jobId, troveSpecs=None):
        self.callback.info('Scanning System')

        self.rtis.wait(allowReboot=False, firstRun=True)

        updJob = self._getUpdateJob(jobId)
        if troveSpecs:
            updJob.prepareUpdate(troveSpecs, test=True)

        status, statusDetail, survey = self.rtis.scan(jobId,
            troveSpecs and updJob.toxml() or None)

        self.callback.info('Scanning Complete')

        return status, statusDetail, survey
