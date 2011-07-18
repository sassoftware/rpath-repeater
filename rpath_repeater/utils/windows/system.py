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

    @cleanup
    def update(self, troveSpecs, jobId):
        self.callback.info('Updating System')

        # Wait for the service to become available.
        self.rtis.wait(allowReboot=False)

        self.callback.info('Retrieving installed software')

        updJob = UpdateJob(self.rtis.flavor, self.rtis.manifest,
            jobId, callback=self.callback)
        updJob.prepareUpdate(troveSpecs)

        if not self.rtis.isInstalled:
            if 'rPathTools:msi' not in updJob:
                error = ('rPathTools:msi is not available for installation, '
                    'can not continue.')
                self.callback.error(error)
                raise errors.UpdateError, error

            self.rtis.applyCriticalUpdate(updJob)

        results = self.rtis.applyUpdate(updJob)

        self.callback.info('Update Complete')
        return results

    @cleanup
    def configure(self, values, jobId):
        self.callback.info('Configuring System')

        self.rtis.wait(allowReboot=False)

        results = self.rtis.applyConfiguration(values, jobId)

        self.callback.info('Configuration Complete')

        return results
