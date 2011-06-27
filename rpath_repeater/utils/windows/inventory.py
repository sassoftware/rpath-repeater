#
# Copyright (c) 2011 rPath, Inc.
#

from wmiclient import WMIFileNotFoundError

from rpath_repeater.utils.windows.callbacks import BaseCallback

def error_handler(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except WMIFileNotFoundError:
            return None
    return wrapper


class Inventory(object):
    _inventory_keypath = r'SOFTWARE\rPath\rTIS.NET\inventory'

    def __init__(self, wmiclient, callback=None):
        self._wmi = wmiclient

        if not callback:
            self.callback = BaseCallback()
        else:
            self.callback = callback

    def setup(self):
        """
        Create the base set of required keys.
        """

        try:
            self._wmi.registryGetKey(self._inventory_keypath, 'local_uuid')
        except WMIFileNotFoundError:
            self.callback.info('Creating Required Registry Keys')
            self._wmi.registryCreateKey('SOFTWARE', 'rPath')
            self._wmi.registryCreateKey(r'SOFTWARE\rPath', 'rTIS.NET')
            self._wmi.registryCreateKey(r'SOFTWARE\rPath\rTIS.NET', 'inventory')
            self.localUUID = ''
            self.generatedUUID = ''

    @property
    @error_handler
    def computerName(self):
        result = self._wmi.registryGetKey(
            r'SYSTEM\CurrentControlSet\Control\ComputerName\ActiveComputerName',
            'ComputerName'
        )

        names = result.output
        assert len(names) == 1
        return names[0].strip()

    @error_handler
    def _get_localUUID(self):
        result = self._wmi.registryGetKey(
            self._inventory_keypath,
            'local_uuid'
        )

        lines = result.output
        if lines:
            assert len(lines) == 1
            return lines[0].strip()
        else:
            return ''

    def _set_localUUID(self, uuid):
        self._wmi.registrySetKey(
            self._inventory_keypath,
            'local_uuid',
            uuid
        )

    localUUID = property(_get_localUUID, _set_localUUID)

    @error_handler
    def _get_generatedUUID(self):
        result = self._wmi.registryGetKey(
            self._inventory_keypath,
            'generated_uuid'
        )

        lines = result.output
        if lines:
            assert len(lines) == 1
            return lines[0].strip()
        else:
            return ''

    def _set_generatedUUID(self, uuid):
        self._wmi.registrySetKey(
            self._inventory_keypath,
            'generated_uuid',
            uuid
        )

    generatedUUID = property(_get_generatedUUID, _set_generatedUUID)

    def _get_uuids(self):
        return self.localUUID, self.generatedUUID

    def _set_uuids(self, localUUID, generatedUUID):
        self.localUUID = localUUID
        self.generatedUUID = generatedUUID

    uuids = property(_get_uuids, _set_uuids)

    @property
    @error_handler
    def networkInfo(self):
        result, interfaces = self._wmi.queryNetwork()
        return interfaces
