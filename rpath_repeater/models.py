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


import sys

from conary import conaryclient
from conary import versions
from conary.lib import util

from rpath_repeater.utils.xmlutils import XML
from smartform import descriptor

from rmake3.core.types import SlotCompare, freezify
from rmake3.lib import chutney

chutney.register(descriptor.ProtectedUnicode)

class ModelMeta(type):
    """Metaclass to automatically register child classes to chutney"""
    def __new__(mcs, name, bases, attrs):
        new_class = type.__new__(mcs, name, bases, attrs)
        # Don't register "private" classes
        if not name.startswith('_'):
            # We need to pass _force here, otherwise chutney will try to
            # import this module and find this class, when it's not created
            # just yet.
            chutney.register(new_class, _force=True)
            frozenType = freezify(new_class)
            # Frozen models belong to the same module as the class
            frozenType.__module__ = new_class.__module__
            module = sys.modules[new_class.__module__]
            module.__dict__[frozenType.__name__] = frozenType
        return new_class

class _Serializable(object):
    _tag = None

    def _getTag(self, tag=None):
        if tag is None:
            return self._tag
        return tag

    def toXmlDom(self, tag=None):
        tag = self._getTag()
        if tag is None:
            return None
        children = []
        for slot in self.__slots__:
            val = getattr(self, slot)
            if val is None:
                continue
            if hasattr(val, 'toXmlDom'):
                val = val.toXmlDom(slot)
                if val is None:
                    continue
                children.append(val)
                continue
            if not isinstance(val, (basestring, int, long, float)):
                continue
            # Assume string
            val = unicode(val)
            # Crude attempt to not doubly-encode xml
            if val.lstrip().startswith('<') and val.rstrip().endswith('>'):
                children.append(XML.CDATA(slot, val))
            else:
                children.append(XML.Text(slot, val))
        return XML.Element(tag, *children)

    def toXml(self):
        dom = self.toXmlDom()
        if dom is None:
            return None
        return XML.toString(dom)

class _SerializableListMixIn(_Serializable):
    def toXmlDom(self, tag=None):
        tag = self._getTag()
        if tag is None:
            return None
        children = (x.toXmlDom() for x in self)
        children = (x for x in children if x is not None)
        return XML.Element(tag, *children)

class _SerializableList(list, _SerializableListMixIn):
    pass


class _BaseSlotCompare(SlotCompare):
    __slots__ = []
    __metaclass__ = ModelMeta
    def toDict(self):
        ret = {}
        for slot in self.__slots__:
            val = getattr(self, slot)
            if val is not None:
                if isinstance(val, SlotCompare):
                    val = val.toDict()
                ret[slot] = val
        return ret

class CimParams(_BaseSlotCompare):
    """
    Information required in order to talk to a WBEM endpoint
    """
    __slots__ = [ 'host', 'port', 'clientCert', 'clientKey', 'requiredNetwork',
        'eventUuid', 'instanceId', 'targetName', 'targetType',
        'launchWaitTime']
    # XXX instanceId, targetName, targetType have nothing to do with
    # CimParams, they should be in a different data structure

class WmiParams(_BaseSlotCompare):
    """
    Information required in order to talk to a WBEM endpoint
    """
    __slots__ = [ 'host', 'port', 'username', 'password', 'domain',
        'eventUuid', ]

class AssimilatorParams(_BaseSlotCompare):
    '''
    Information required to assimilate a Linux system via SSH
    '''
    # sshAuth is a list of hashes to try, like so:
    # [{ 'sshUser' : user, 'sshPassword' : pass, 'sshKey' : key_path_or_bytes }, {...}, ...]
    # caCert are the contents of the cert
    # platformLabels is a list of key value pairs [('centos-5',  label), ...]
    __slots__ = [ 'host', 'port', 'caCert', 'sshAuth', 'platformLabels', 'projectLabel', 'installTrove', 'eventUuid' ]

class ManagementInterfaceParams(_BaseSlotCompare):
    """
    Information needed for probing for a management interface (e.g. WMI,
    WBEM)
    """
    __slots__ = [ 'host', 'interfacesList', 'eventUuid', ]

class URL(_BaseSlotCompare):
    """
    Basic representation of a URL
    """
    __slots__ = [ 'scheme', 'username', 'password', 'host', 'port',
        'path', 'query', 'fragment', 'unparsedPath', 'headers', ]

    def asString(self):
        return util.urlUnsplit((self.scheme, self.username, self.password,
            self.host, self.port, self.path, self.query,
            self.fragment)).encode('ascii')

    @classmethod
    def fromString(cls, url, host=None, port=None):
        arr = util.urlSplit(url)
        o = cls()
        (o.scheme, o.username, o.password, o.host, o.port,
            o.path, o.query, o.fragment) = arr
        o.unparsedPath = util.urlUnsplit((None, None, None, None, None,
            o.path, o.query, o.fragment))
        if o.host is None:
            o.host = host
        if o.port is None:
            o.port = port
        return o

class ResultsLocation(URL):
    """
    Results will be posted to this location
    """

class ImageFile(_BaseSlotCompare, _Serializable):
    __slots__ = [ 'title', 'size', 'sha1', 'file_name', 'url', 'destination', ]
    _tag = "file"

class Image(_BaseSlotCompare):
    __slots__ = [ 'name', 'architecture', 'files', 'metadata', ]

class ImageFiles(_SerializableList):
    _tag = "files"

class ImageRef(_BaseSlotCompare, _Serializable):
    __slots__ = ['id']
    _tag = "image"

class Trove(_BaseSlotCompare, _Serializable):
    __slots__ = ( 'name', 'version', 'flavor', )
    _tag = "trove"

    @classmethod
    def fromTroveSpec(cls, troveSpec):
        n, v, f = conaryclient.cmdline.parseTroveSpec(troveSpec)
        thawed_v = versions.ThawVersion(v)
        return cls.fromNameVersionFlavor(n, thawed_v, f)

    @classmethod
    def fromNameVersionFlavor(cls, name, version, flavor):
        obj = cls()
        obj.name = name
        obj.version = Version.fromVersionFlavor(version, flavor)
        obj.flavor = Version.sanitizeFlavor(flavor)
        return obj

class Version(_BaseSlotCompare, _Serializable):
    __slots__ = ( 'full', 'label', 'revision', 'ordering', 'flavor', )
    _tag = "version"

    @classmethod
    def fromVersionFlavor(cls, version, flavor):
        nobj = cls()
        nobj.full = str(version)
        nobj.ordering = str(version.timeStamps()[0])
        nobj.revision = str(version.trailingRevision())
        nobj.label = str(version.trailingLabel())
        nobj.flavor = cls.sanitizeFlavor(flavor)
        return nobj

    @classmethod
    def sanitizeFlavor(cls, flavor):
        if flavor is None:
            return ""
        return str(flavor)

class Response(_BaseSlotCompare):
    __slots__ = ['response', ]

class Target(_BaseSlotCompare, _Serializable):
    __slots__ = []
    _tag = 'target'

class TargetConfiguration(_BaseSlotCompare):
    __slots__ = ['targetType', 'targetName', 'alias', 'config',]

class TargetUserCredentials(_BaseSlotCompare):
    __slots__ = ['rbUser', 'rbUserId', 'isAdmin', 'credentials', 'opaqueCredentialsId', ]

class TargetCommandArguments(_BaseSlotCompare):
    __slots__ = ['jobUrl', 'authToken',
        'targetConfiguration', 'targetUserCredentials', 'args',
        'targetAllUserCredentials', 'zoneAddresses', ]

class ScriptOutput(_BaseSlotCompare, _Serializable):
    __slots__ = [ 'returnCode', 'stdout', 'stderr' ]
    _tag = 'scriptOutput'
