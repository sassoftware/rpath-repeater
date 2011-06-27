#
# Copyright (c) 2011 rPath, Inc.
#

import os
import copy
import itertools
from collections import namedtuple

from conary import files
from conary import trove
from conary import conarycfg
from conary import conaryclient
from conary.trovetup import TroveTuple
from conary.conaryclient import cml
from conary.conaryclient import cmdline
from conary.conaryclient import modelupdate
from conary.errors import TroveSpecsNotFound

from rpath_repeater.utils.windows.rtis import Servicing
from rpath_repeater.utils.windows.callbacks import BaseCallback

class CapsuleContents(namedtuple('FileContents', 'name info msi nvf content operation')):
    __slots__ = ()


class UpdateJob(object):
    """
    Class to represent a Windows update.
    """

    CRITICAL_PACKAGES = ('rPathTools', 'rPathTools:msi', )

    def __init__(self, systemFlavor, manifest, jobId, callback=None,
        copy=False):
        if not callback:
            self.callback = BaseCallback()
        else:
            self.callback = callback

        self._systemFlavor = systemFlavor
        self._jobId = jobId

        self._uJob = None
        self._newPollingManifest = None
        self._newSystemModel = None
        self._contents = {}

        if copy:
            return

        self._cfg = conarycfg.ConaryConfiguration(True)
        self._cfg.initializeFlavors()
        self._cfg.dbPath = ':memory:'
        self._cfg.flavor = [self._systemFlavor, ]
#        self._cfg.readUrl('http://localhost.localdomain/conaryrc')
        self._cfg.readUrl('http://dhcp224.eng.rpath.com/conaryrc')

        self._client = conaryclient.ConaryClient(self._cfg)

        self._model_cache = modelupdate.CMLTroveCache(
            self._client.getDatabase(),
            self._client.getRepos(),
        )

        self._manifest = [ TroveTuple(*x) for x in manifest ]

        self._populateDatabase()

    def _populateDatabase(self):
        db = self._client.getDatabase()

        try:
            troves = self._model_cache.getTroves(self._manifest)
        except TroveSpecsNotFound:
            self.callback.error('This system is associated with an '
                'appliance that can not be accessed: %s' % (self._manifest, ))
            raise

        for trv in troves:
            trvId = db.addTrove(trv)
            db.addTroveDone(trvId)
        db.commit()

    def __copy__(self):
        cls = self.__class__
        obj = cls(self._systemFlavor, None, self._jobId, callback=self.callback,
            copy=True)
        obj._cfg = self._cfg
        obj._client = self._client
        obj._model_cache = self._model_cache
        obj._manifest = self._manifest
        return obj

    def __iter__(self):
        return iter(self._updates)

    def __contains__(self, name):
        return name in [ x[0] for x in self._updates ]

    @property
    def system_model(self):
        return self._newSystemModel

    @property
    def manifest(self):
        return self._manifest

    @property
    def polling_manifest(self):
        return self._newPollingManifest

    @property
    def jobId(self):
        return 'job-%s' % self._jobId

    def getCriticalJob(self):
        """
        Extract any components that are considered "critical".
        """

        if not self._uJob:
            raise RuntimeError, 'Must prepare job first'

        # get all of the critical updates out of the current job
        updates = []
        for update in self._updates:
            if update[0] in self.CRITICAL_PACKAGES:
                updates.append(update)

        # remove all of hte critical updates from the current job
        for update in updates:
            self._updates.remove(update)

        # stop if there are no critical updates
        if not updates:
            return False

        cJob = copy.copy(self)
        cJob._updates = updates

        cJob._newSystemModel = [ 'install %s=%s' % (x[0], x[2][0])
            for x in cJob._updates ]

        cJob._newPollingManifest = [ '%s=%s[%s]' % (x[0], x[2][0], x[2][1])
            for x in cJob._updates ]

        return cJob

    def getUpdateJob(self, system_model):
        """
        Build conary update job that represents required changes.
        """

        model = cml.CML(self._cfg)
        model.parse(system_model)

        updJob = self._client.newUpdateJob()
        troveSetGraph = self._client.cmlGraph(model)
        self._client._updateFromTroveSetGraph(updJob, troveSetGraph,
            self._model_cache)
        return updJob

    def prepareUpdate(self, updateTroveSpecs):
        """
        Lookup update information from the conary repository to figure out what
        needs to be changed on the remote system.
        """

        # NOTE: This assumes that updateTroveSpecs will only ever be top
        #       level items.

        self.callback.info('Preparing updates')

        newTroveSpecs = [ cmdline.parseTroveSpec(x)
            for x in updateTroveSpecs if x ]

        newTroveTups = self._client.repos.findTroves(None, newTroveSpecs)
        newTroveTups = [ TroveTuple(x) for x in
            itertools.chain(*newTroveTups.values()) ]

        self._newSystemModel = [ 'install %s=%s' % (x.name, x.version)
            for x in newTroveTups ]

        self._newPollingManifest = [ x.asString()
            for x in newTroveTups ]

        self._uJob = self.getUpdateJob(self._newSystemModel)

        self._updates = []
        for job in self._uJob.getJobs():
            for update in job:
                self._updates.append(update)

        return self._newSystemModel

    def getFileContents(self):
        """
        Get all of the capsule contents required to complete the update.
        """

        assert self._updates

        if self._contents:
            return self._contents

        self.callback.info('Retrieving file contents')

        cs = self._client.repos.createChangeSet(self._updates, withFiles=True,
            withFileContents=True)

        names = [ x[0] for x in self._updates ]

        for trvCs in cs.iterNewTroveList():
            if trvCs.getName() not in names:
                self.callback.debug('skipping %s since it was not in the '
                    'request' % trvCs.getName())
                continue

            nvf = TroveTuple(
                trvCs.getName(),
                trvCs.getNewVersion(),
                trvCs.getNewFlavor()
            )

            for pathId, path, fileId, fileVer in trvCs.getNewFileList():
                if pathId != trove.CAPSULE_PATHID:
                    continue

                fileStream = cs.getFileChange(None, fileId)
                if not files.frozenFileHasContents(fileStream):
                    continue

                name = os.path.basename(path)
                fileInfo = files.ThawFile(fileStream, pathId)

                cfile = cs.getFileContents(pathId, fileId, compressed=False)
                contents = cfile[1].get()

                self._contents[nvf] = CapsuleContents(name, fileInfo,
                    trove.Trove(trvCs).troveInfo.capsule.msi, nvf, contents,
                    Servicing.operations.UPDATE)

                break

            # This is a package update with no contents.
            else:
                self._contents[nvf] = CapsuleContents(None, None, None, nvf,
                    None, Servicing.operations.UPDATE)

        # Retrieve trove info for all delete jobs
        nvfs = []
        for name, (oldVer, oldFlv), (newVer, newFlv), _ in self:
            if newVer is not None or newFlv is not None:
                continue
            nvfs.append(TroveTuple(name, oldVer, oldFlv))

        ti = self._client.repos.getTroveInfo(trove._TROVEINFO_TAG_CAPSULE, nvfs)

        for nvf, capsule in itertools.izip(nvfs, ti):
            if capsule is not None:
                capsule = capsule.msi
            self._contents.setdefault(nvf,
                CapsuleContents(None, None, capsule, nvf, None,
                    Servicing.operations.ERASE))

        return self._contents
