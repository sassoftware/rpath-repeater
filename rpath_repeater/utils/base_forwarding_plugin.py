#
# Copyright (c) 2010 rPath, Inc.
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

import tempfile

from xml.dom import minidom
from conary import conaryclient
from conary import versions

from twisted.web import client

from rmake3.core import handler
from rmake3.core import plug_dispatcher
from rmake3.worker import plug_worker

PREFIX = 'com.rpath.sputnik'

class BaseForwardingPlugin(plug_dispatcher.DispatcherPlugin,
                           plug_worker.WorkerPlugin):
    pass


def exposed(func):
    """Decorator that exposes a method as being externally callable"""
    func.exposed = True
    return func


class Options(object):
    __slots__ = [ 'exposed', ]
    def __init__(self):
        self.exposed = set()

    def addExposed(self, name):
        self.exposed.add(name)


class BaseHandler(handler.JobHandler):
    class __metaclass__(type):
        def __new__(cls, name, bases, attrs):
            ret = type.__new__(cls, name, bases, attrs)
            ret.Meta = Options()
            for attrName, attrVal in attrs.items():
                if getattr(attrVal, 'exposed', None):
                    ret.Meta.addExposed(attrName)
            return ret

    def setup (self):
        pass

    def initCall(self):
        self.data = self.getData().thaw().getDict()
        self.zone = self.data.pop('zone', None)
        self.method = self.data['method']
        self.methodArguments = self.data.pop('methodArguments', {})
        self.resultsLocation = self.data.pop('resultsLocation', {})
        self.eventUuid = self.data.pop('eventUuid', None)


class BaseTaskHandler(plug_worker.TaskHandler):
    TemporaryDir = "/dev/shm"

    @classmethod
    def _tempFile(cls, prefix, contents):
        # NamedTemporaryFile will conveniently go *poof* when it gets closed
        tmpf = tempfile.NamedTemporaryFile(dir=cls.TemporaryDir, prefix=prefix)
        tmpf.write(contents)
        # Flush the contents on the disk, so python's ssl lib can see them
        tmpf.flush()
        return tmpf

    @classmethod
    def _trove(cls, si):
        Text = XML.Text
        nvf = "%s=%s" % (si['name'], si['VersionString'])
        n, v, f = conaryclient.cmdline.parseTroveSpec(nvf)

        name = Text("name", n)
        version = cls._version(v, f)
        flavor = Text("flavor", str(f))

        return XML.Element("trove", name, version, flavor)

    @classmethod
    def _version(cls, v, f):
        thawed_v = versions.ThawVersion(v)
        Text = XML.Text
        full = Text("full", str(thawed_v))
        ordering = Text("ordering", thawed_v.timeStamps()[0])
        revision = Text("revision", str(thawed_v.trailingRevision()))
        label = Text("label", str(thawed_v.trailingLabel()))
        flavor = Text("flavor", str(f))
        return XML.Element("version", full, label, revision, ordering, flavor)


class XML(object):
    @classmethod
    def Text(cls, tagName, text):
        txt = minidom.Text()
        txt.data = text
        return cls.Element(tagName, txt)

    @classmethod
    def Element(cls, tagName, *children, **attributes):
        node = cls._Node(tagName, minidom.Element)
        for child in children:
            node.appendChild(child)
        for k, v in attributes.items():
            node.setAttribute(k, unicode(v).encode("utf-8"))
        return node

    @classmethod
    def _Node(cls, tagName, factory):
        node = factory(tagName)
        return node

class HTTPClientFactory(client.HTTPClientFactory):
    def __init__(self, url, *args, **kwargs):
        client.HTTPClientFactory.__init__(self, url, *args, **kwargs)
        self.status = None
        self.deferred.addCallback(
            lambda data: (data, self.status, self.response_headers))
