#!/usr/bin/python
#
# Copyright (c) 2012 rPath, Inc.
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

from xml.etree import cElementTree as etree

class Formatter(object):
    __slots__ = [ 'jobs', 'root', 'changes' ]
    def __init__(self, updateJob):
        self.jobs = []
        if updateJob is not None:
            self.jobs = updateJob.getJobs()
        self.root = None
        self.changes = None

    def format(self):
        self.root = etree.Element('preview')
        self.changes = etree.SubElement(self.root, 'conary_package_changes')
        for oneJob in self.jobs:
            for j in oneJob:
                self._formatJob(j)

    def toxml(self):
        return etree.tostring(self.root)

    def _formatJob(self, job):
        (name, (oldVer, oldFla), (newVer, newFla)) = job[:3]
        if oldVer is None:
            self._formatInstall(name, newVer, newFla)
        elif newVer is None:
            self._formatErase(name, oldVer, oldFla)
        else:
            self._formatUpdate(name, oldVer, oldFla, newVer, newFla)

    def _formatInstall(self, name, version, flavor):
        node = self._newPackageChange('added')
        self._packageSpec(node, 'added_conary_package', name, version, flavor)

    def _formatErase(self, name, version, flavor):
        node = self._newPackageChange('removed')
        self._packageSpec(node, 'removed_conary_package', name, version, flavor)

    def _formatUpdate(self, name, oldVersion, oldFlavor, newVersion, newFlavor):
        node = self._newPackageChange('changed')
        self._packageSpec(node, 'from', name, oldVersion, oldFlavor)
        self._packageSpec(node, 'to', name, newVersion, newFlavor)
        diff = etree.SubElement(node, 'conary_package_diff')
        self._fieldDiff(diff, 'version', oldVersion, newVersion)
        self._fieldDiff(diff, 'flavor', oldFlavor, newFlavor)

    def _newPackageChange(self, type):
        node = etree.SubElement(self.changes, 'conary_package_change')
        etree.SubElement(node, 'type').text = type
        return node

    def _packageSpec(self, parent, tag, name, version, flavor):
        node = etree.SubElement(parent, tag)
        etree.SubElement(node, 'name').text = str(name)
        etree.SubElement(node, 'version').text = str(version)
        etree.SubElement(node, 'flavor').text = str(flavor)
        return node

    def _fieldDiff(self, parent, tag, oldValue, newValue):
        if oldValue == newValue:
            return
        node = etree.SubElement(parent, tag)
        etree.SubElement(node, 'from').text = str(oldValue)
        etree.SubElement(node, 'to').text = str(newValue)

