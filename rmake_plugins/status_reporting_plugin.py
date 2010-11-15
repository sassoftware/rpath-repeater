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

import logging

from twisted.internet import error as internet_error
from twisted.internet import reactor
from rmake3.core import plug_dispatcher
from rmake3.lib import netlink
from rpath_repeater.utils import base_forwarding_plugin as bfp

log = logging.getLogger(__name__)

class NodeReportingPlugin(plug_dispatcher.DispatcherPlugin):
    HEARTBEAT = 60
    def dispatcher_post_setup(self, dispatcher):
        self.setUpHeartbeat(dispatcher)
        self.getLocalAddressses(dispatcher)

    def setUpHeartbeat(self, dispatcher):
        reactor.callLater(self.HEARTBEAT, self.setUpHeartbeat, dispatcher)
        self.syncWorkers(dispatcher)

    def dispatcher_worker_up(self, dispatcher, worker):
        self.syncWorkers(dispatcher)

    def dispatcher_worker_down(self, dispatcher, worker):
        self.syncWorkers(dispatcher)

    def getLocalAddressses(self, dispatcher):
        rtnl = netlink.RoutingNetlink()
        self.localAddresses = set(x[1] for x in rtnl.getAllAddresses())
        return self.localAddresses

    def syncWorkers(self, dispatcher):
        children = []
        for workerJid, worker in dispatcher.workers.items():
            children.append(self._getWorkerData(worker))
        if not children:
            # Cowardly refuse to update the list if all nodes are marked as
            # being down
            return
        node = bfp.XML.Element('management_nodes', *children)
        data = self.toXml(node)
        self.postResults(data)

    @classmethod
    def toXml(self, elt):
        return bfp.XML.toString(elt)

    def _getWorkerData(self, worker):
        E = bfp.XML.Element
        T = bfp.XML.Text
        children = []
        children.append(T("node_jid", worker.jid.full()))
        if worker.zoneNames:
            # We only support one zone per management node
            children.append(E('zone', T('name', worker.zoneNames[0])))
        ipv4, ipv6 = self._splitAddressTypes(worker.addresses)
        isLocal = str(bool(self.localAddresses.intersection(worker.addresses))).lower()
        networks = [ E("network",
            T("ip_address", x), T("dns_name", x), T("device_name", "eth0"))
            for x in sorted(ipv4) ]
        networks.extend(E("network",
            T("ipv6_address", x), T("dns_name", x), T("device_name", "eth0"))
            for x in sorted(ipv6))
        children.append(E("networks", *networks))
        children.append(T("local", isLocal))
        node = E("management_node", *children)
        return node

    @classmethod
    def _splitAddressTypes(cls, addresses):
        "Separate ipv4 and ipv6 addresses"
        ipv4 = set()
        ipv6 = set()
        for addr in addresses:
            if ':' in addr:
                ipv6.add(addr)
            else:
                ipv4.add(addr)
        return ipv4, ipv6

    def postResults(self, data):
        path = "/api/inventory/management_nodes"
        host = "localhost"
        port = 80
        headers = {
            'Content-Type' : 'application/xml; charset="utf-8"',
            'Host' : host, }
        agent = "rmake-plugin/1.0"
        fact = bfp.HTTPClientFactory(path, method='PUT', postdata=data,
            headers = headers, agent = agent)
        @fact.deferred.addCallback
        def processResult(result):
            print "Received result for %s: %s" % (host, result)
            return result

        @fact.deferred.addErrback
        def processError(error):
            err = error.value
            if isinstance(err, internet_error.ConnectionDone):
                return
            print "Error: %s" % error.getErrorMessage()

        reactor.connectTCP(host, port, fact)

