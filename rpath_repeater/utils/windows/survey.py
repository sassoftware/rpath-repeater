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


import copy
from lxml import etree
from lxml.builder import ElementMaker

from rpath_tools.client.sysdisco.packages import IDFactory
from rpath_tools.client.sysdisco.packages import ConaryScanner as _ConaryScanner
from rpath_tools.client.sysdisco.packages import WindowsScanner as _WindowsScanner
from rpath_tools.client.utils.config_descriptor_cache import ConfigDescriptorCache

def children(node):
    return dict((x.tag, x) for x in node.iterchildren())


class DbShim(object):
    def __init__(self, repos, pkglist):
        self.repos = repos
        self.pkglist = [ x[0] for x in pkglist ]
        self._manifest = dict((x, y) for x, y in pkglist)

    def iterAllTroves(self):
        return iter(self.pkglist)

    def getTrove(self, name, version, flavor):
        trv = self.repos.getTrove(name, version, flavor)
        install_time = self._manifest.get((name, version, flavor))
        trv.troveInfo.installTime = lambda: install_time
        return trv


class ConaryScanner(_ConaryScanner):
    def __init__(self, pkglist, client):
        _ConaryScanner.__init__(self)
        self.pkglist = pkglist
        self._client = client

    def _getDb(self):
        return DbShim(self.client.repos, self.pkglist)


class WindowsPackage(object):
    __slots__ = ('node', 'pkgInfo', )

    def __init__(self, node):
        self.node = node
        self.pkgInfo = children(self.node).get('windows_package_info')

    @property
    def id(self):
        return self.node.attrib['id']

    @property
    def name(self):
        return children(self.pkgInfo).get('product_name').text

    @property
    def version(self):
        return children(self.pkgInfo).get('version').text

    @property
    def type(self):
        return children(self.pkgInfo).get('type').text

    @property
    def productCode(self):
        return children(self.pkgInfo).get('product_code').text

    def toxml(self):
        return self.node

    def __hash__(self):
        return hash(self.productCode)

    def __cmp__(self, other):
        assert isinstance(other, WindowsPackage)

        code = cmp(self.productCode, other.productCode)
        if code != 0:
            return code

        return cmp(self.name, other.name)


class WindowsScanner(_WindowsScanner):
    def __init__(self, packages):
        _WindowsScanner.__init__(self)
        self.packages = packages

    def scan(self):
        self._results = {}
        for node in self.packages.iterchildren():
            pkg = WindowsPackage(node)
            self._results[pkg.productCode] = pkg
        return self._results


class Survey(object):
    """
    Class for adding any additional information to a Windows survey.
    """

    def __init__(self, rtis, survey_data, updJobXml=None):
        self.rtis = rtis
        self.data = survey_data
        self.updJobXml = updJobXml

        self.e = ElementMaker()

    def tostring(self, prettyPrint=False):
        root = self.e.surveys()
        root.append(self.data)
        return etree.tostring(root, pretty_print=prettyPrint)

    def _getConaryClient(self):
        # Import here to avoid import loop
        from rpath_repeater.utils.windows.updates import UpdateJob
        return UpdateJob(self.rtis.flavor, self.rtis.manifest,
            self.rtis.system_model, None, callback=self.rtis.callback)._client

    def addComputedInformation(self):
        self.addPackageInformation()
        self.addSystemModel()
        self.addPreview()
        self.addConfigurationDescriptor()

    def addPackageInformation(self):
        """
        Add the conary package information to the survey.
        """

        # 1. get the conary package data from the target system
        # 2. get troveInfo for all packages from teh repository
        # 3. map conary packages to windows packages and vice versa

        manifest = self.rtis.manifest
        conaryInfo = ConaryScanner(manifest, self._getConaryClient()).scan()
        productCodes = dict((x.msi.productCode, x)
            for x in conaryInfo.itervalues() if x.msi)

        windowsPkgs = children(self.data).get('windows_packages')
        windowsInfo = WindowsScanner(windowsPkgs).scan()

        idGen = IDFactory()

        conary_packages = self.e.conary_packages()
        for pkg in conaryInfo.itervalues():
            nodeId = idGen.getId(pkg)
            # toxml here is a misnomer, it really returns an etree node
            node = pkg.toxml(nodeId)
            winPkg = windowsInfo.get(pkg.msi.productCode) if pkg.msi else None
            if winPkg:
                child = node.find('.//conary_package_info')
                child.append(copy.copy(winPkg.node))
            conary_packages.append(node)

        windows_packages = self.e.windows_packages()
        for pkg in windowsInfo.itervalues():
            node = pkg.node
            cnyPkg = productCodes.get(pkg.productCode)
            if cnyPkg:
                node.append(self.e.conary_package(id=idGen.getId(cnyPkg)))
            node.append(self.e.encapsulated(str(bool(cnyPkg)).lower()))
            windows_packages.append(node)

        self.data.remove(children(self.data).get('windows_packages'))
        if 'conary_packages' in children(self.data):
            self.data.remove(children(self.data).get('conary_packages'))
        self.data.append(conary_packages)
        self.data.append(windows_packages)

    def addSystemModel(self):
        node = children(self.data).get('system_model')
        if not node:
            node = self.e.system_model()
            self.data.append(node)

        node.append(self.e.content('\n'.join(self.rtis.system_model)))

    def addPreview(self):
        if self.updJobXml:
            node = etree.fromstring(self.updJobXml)
            self.data.append(node)

    def addConfigurationDescriptor(self):
        # get installed software
        manifest = self.rtis.manifest

        # Find the top level group.
        groups = [ x[0] for x in manifest
            if x[0].name.startswith('group-') and 
               x[0].name.endswith('-appliance') ]

        # if no top level group abort
        if not groups:
            return

        group = groups[0]

        # Get the config descriptor from the repository.
        repos = self._getConaryClient().repos
        desc = ConfigDescriptorCache(repos).getDescriptor(group)
        if not desc:
            return

        desc.setDisplayName('Configuration Descriptor')
        desc.addDescription('Configuration Descriptor')

        chldrn = children(self.data)
        if 'config_properties_descriptor' in chldrn:
            self.data.remove(chldrn.get('config_properties_descriptor'))

        node = self.e.config_properties_descriptor(
                                    etree.fromstring(
                                        desc.toxml(validate=False)))
        self.data.append(node)
