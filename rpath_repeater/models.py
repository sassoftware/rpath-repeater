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

from rpath_repeater.utils.xmlutils import XML

from rmake3.core.types import SlotCompare
from rmake3.lib import chutney

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
        return new_class

class _Serializable(object):
    _tag = None

    def _getTag(self, tag=None):
        if tag is None:
            return self._tag
        return tag

    def _toXmlDom(self, tag=None):
        tag = self._getTag()
        if tag is None:
            return None
        children = []
        for slot in self.__slots__:
            val = getattr(self, slot)
            if val is None:
                continue
            if hasattr(val, '_toXmlDom'):
                val = val._toXmlDom(slot)
                if val is None:
                    continue
                children.append(val)
                continue
            if not isinstance(val, basestring):
                continue
            # Assume string
            children.append(XML.Text(slot, unicode(val)))
        return XML.Element(tag, *children)

    def toXml(self):
        dom = self._toXmlDom()
        if dom is None:
            return None
        return XML.toString(dom)

class _SerializableList(list, _Serializable):
    def _toXmlDom(self, tag=None):
        tag = self._getTag()
        if tag is None:
            return None
        children = (x._toXmlDom() for x in self)
        children = (x for x in children if x is not None)
        return XML.Element(tag, *children)


class _BaseSlotCompare(SlotCompare):
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
    __slots__ = [ 'host', 'port', 'clientCert', 'clientKey', 
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
        port = self.port
        if self.scheme == "http":
            port = port or 80
        elif self.scheme == "https":
            port = port or 443
        url = "%s://%s:%s%s" % (self.scheme, self.host, port,
            self.unparsedPath)
        return url.encode('ascii')

class ResultsLocation(URL):
    """
    Results will be posted to this location
    """

class ImageFile(_BaseSlotCompare, _Serializable):
    __slots__ = [ 'title', 'size', 'sha1', 'fileName', 'url', 'destination', ]
    _tag = "file"

class ImageMetadata(_BaseSlotCompare, _Serializable):
    __slots__ = [ 'owner', 'billingCode', 'deptCode', 'cost', ]
    _tag = "metadata"

class Image(_BaseSlotCompare):
    __slots__ = [ 'name', 'architecture', 'files', 'metadata', ]

class ImageFiles(_SerializableList):
    _tag = "files"

if __name__ == '__main__':
    files = ImageFiles([ ImageFile(title="i1", sha1="s1"),
        ImageFile(title="i2", sha1="s2") ])
    metadata = ImageMetadata(owner="me")
    files.append(metadata)
    print metadata.toXml()
    print files.toXml()
