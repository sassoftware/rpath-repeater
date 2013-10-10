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


import logging

from twisted.internet import error as internet_error
from twisted.internet import reactor
from rmake3.core import plug_dispatcher
from rmake3.lib import netlink
from rmake3.lib.twisted_extras import tools
from rpath_repeater.utils import base_forwarding_plugin as bfp
from rpath_repeater.utils import http

log = logging.getLogger(__name__)

class NodeReportingPlugin(plug_dispatcher.DispatcherPlugin):
    HEARTBEAT = 600
    TIMEOUT = 60


    def dispatcher_post_setup(self, dispatcher):
        self.serializer = tools.Serializer()
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
        log.debug("Updating inventory with %d management nodes", len(children))
        return self.serializer.call(self.postResults, (data,), collapsible=True)

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
        # Prefer the ipv4 address for display purposes
        if ipv4:
            hname = min(ipv4)
        else:
            hname = min(ipv6)
        children.append(T("hostname", "rPath Update Service - %s" % hname))
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
        path = "/api/v1/inventory/management_nodes"
        host = "localhost"
        port = 80
        headers = {
            'Content-Type' : 'application/xml; charset="utf-8"',
            'Host' : host, }
        fact = http.HTTPClientFactory(path, method='PUT', postdata=data,
            headers=headers, timeout=self.TIMEOUT)
        @fact.deferred.addCallback
        def processResult(result):
            log.debug("Management node list updated")
            return result

        @fact.deferred.addErrback
        def processError(error):
            err = error.value
            if isinstance(err, internet_error.ConnectionDone):
                return
            log.error("Unable to update management node list: %s",
                    error.getErrorMessage())

        reactor.connectTCP(host, port, fact)
        return fact.deferred
