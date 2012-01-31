#
# Copyright (c) 2011 rPath, Inc.
#

import os
import time
import statvfs
from collections import namedtuple

from lxml import etree
from lxml.builder import ElementMaker

from conary.lib import util
from conary.deps import deps
from conary.trovetup import TroveTuple

from wmiclient import WMIBaseError
from wmiclient import WMIFileNotFoundError

from rpath_repeater.utils.windows.callbacks import BaseCallback
from rpath_repeater.utils.windows.errors import NotEnoughSpaceError
from rpath_repeater.utils.windows.errors import MSIInstallationError
from rpath_repeater.utils.windows.errors import ServiceFailedToStartError

def _filter(func):
    """
    Filter out empty strings from a list of results.
    """
    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)
        if not result:
            return result
        return [ x for x in result if x != '' ]
    return wrapper


class Servicing(object):
    """
    Class for parsing and generating servicing.xml.
    """

    FILENAME = 'servicing.xml'
    LOGFILE = 'setup.log'

    class operations(object):
        UPDATE = 'install'
        ERASE = 'uninstall'

    def __init__(self):
        self.e = ElementMaker()
        self.updateJobs = self.e.updateJobs()
        self.root = self.e.update(
            self.e.logFile(self.LOGFILE),
            self.updateJobs,
        )

        self._cur = None

        self._job_count = 0
        self._subjob_count = 0

    def _addJob(self):
        self._cur.append(self.e.sequence(str(self._job_count)))
        self._cur.append(self.e.logFile(self.LOGFILE))
        self.updateJobs.append(self._cur)
        self._job_count += 1

    @classmethod
    def createUpdateJob(cls):
        obj = cls()
        obj._cur = obj.e.updateJob()
        obj._addJob()
        pkgs = obj.e.packages()
        obj._cur.append(pkgs)
        obj._cur = pkgs
        return obj

    @classmethod
    def createConfigJob(cls):
        obj = cls()
        obj._cur = obj.e.configJob()
        obj._addJob()
        values = obj.e.values()
        obj._cur.append(values)
        obj._cur = values
        return obj

    def addPackage(self, capsule, oldNVF):
        pkg = self.e.package(
            self.e.sequence(str(self._subjob_count)),
            self.e.logFile(self.LOGFILE),
            self.e.operation(capsule.operation),
        )

        if capsule.operation == self.operations.UPDATE:
            pkg.append(self.e.manifestEntry(
                capsule.nvf.asString(withTimestamp=True)))
            if oldNVF is not None:
                pkg.append(self.e.previousManifestEntry(
                    oldNVF.asString(withTimestamp=True)))
            else:
                pkg.append(self.e.previousManifestEntry())
        else:
            pkg.extend([
                self.e.previousManifestEntry(
                    capsule.nvf.asString(withTimestamp=True)),
                self.e.manifestEntry(),
            ])

        if ':' in capsule.nvf.name:
            pkg.extend([
                self.e.type('msi'),
                self.e.productCode(capsule.msi.productCode()),
                self.e.productName(capsule.msi.name()),
                self.e.productVersion(capsule.msi.version()),
                self.e.msiArguments(capsule.msi.msiArgs()),
            ])

            if capsule.operation == self.operations.UPDATE:
                pkg.append(self.e.file(capsule.name))
        else:
            pkg.append(self.e.type('package'))

        if capsule.nvf.name == 'rPathTools:msi':
            pkg.append(self.e.critical('true'))
        else:
            pkg.append(self.e.critical('false'))

        self._cur.append(pkg)
        self._subjob_count += 1

    def addValue(self, value):
        self._cur.append(value)

    def tostring(self, prettyPrint=False):
        return etree.tostring(self.root, pretty_print=prettyPrint)

    @staticmethod
    def c2d(node):
        return dict((x.tag, x) for x in node.iterchildren())

    @staticmethod
    def _handle_unicode_header(fobj):
        header = fobj.read(3)
        if header != '\xef\xbb\xbf':
            fobj.seek(0)
        return fobj

    def iterpackageresults(self, fobj):
        root = etree.parse(self._handle_unicode_header(fobj)).getroot()
        updateJobs = self.c2d(root).get('updateJobs')

        for update in updateJobs.iterchildren():
            for package in self.c2d(update).get('packages').iterchildren():
                info = self.c2d(package)
                operation = info.get('operation').text

                if operation == self.operations.UPDATE:
                    trvSpec = info.get('manifestEntry')
                else:
                    trvSpec = info.get('previousManifestEntry')

                if info.get('packageStatus') is None:
                    status = {}
                else:
                    status = dict((x.tag, x.text)
                        for x in info.get('packageStatus').iterchildren())

                yield operation, trvSpec.text, status

    def iterconfigresults(self, fobj):
        root = etree.parse(self._handle_unicode_header(fobj)).getroot()
        updateJobs = self.c2d(root).get('updateJobs')

        for config in updateJobs.iterchildren():
            handlers = self.c2d(config).get('handlers')
            if not handlers:
                continue
            for hdlr in handlers.iterchildren():
                yield int(hdlr.exitCode), hdlr.name, hdlr.exitCodeDescription


class Package(namedtuple('Package', 'name version product_code')):
    __slots__ = ()


class Packages(object):
    """
    Parse a packages.xml from a remote Windows system into a nice structure.
    """

    FILENAME = 'products.xml'

    def __init__(self):
        self.packages = []

    @classmethod
    def parse(cls, fobj):
        """
        Parse the file object.
        """

        _handle_unicode_header = Servicing._handle_unicode_header
        c2d = Servicing.c2d

        obj = cls()

        # Strip any utf-8 header garbage
        fobj = _handle_unicode_header(fobj)

        # Parse the xml
        root = etree.parse(fobj).getroot()

        for package in root.iterchildren():
            node = c2d(package)
            pkg = Package(node['name'].text, node['version'].text,
                node['productCode'].text)
            obj.packages.append(pkg)

        return obj

    def __contains__(self, pcode):
        return pcode in [ x.product_code for x in self.packages ]


class rTIS(object):
    """
    Representation of all interactions with the remote Windows system with
    regaurd to installing and managing software.
    """

    _service_name = 'rPath Tools Installer Service'
    _params_keypath = r'SOFTWARE\rPath\rTIS.NET\parameters'
    _conary_keypath = r'SOFTWARE\rPath\rTIS.NET\conary'

    _reboot_timeout = 600 # in seconds
    _query_sleep = 1

    # success, restart initiated, restart required
    _success = (0, 1641, 3010)

    def __init__(self, wmiclient, smbclient, callback=None):
        self._wmi = wmiclient
        self._smb = smbclient

        if not callback:
            self.callback = BaseCallback()
        else:
            self.callback = callback

        self._updatesDir = None
        self._flavor = None
        self._runcount = None

    def _sleep(self):
        time.sleep(self._query_sleep)

    def _query(self, func, *args, **kwargs):
        retries = kwargs.pop('retries', 10)
        default = kwargs.pop('default', None)
        raiseErrors = kwargs.pop('raiseErrors', False)
        queries = 0

        result = None
        while not result:
            try:
                queries += 1
                result = func(*args, **kwargs)
            except WMIFileNotFoundError:
                if raiseErrors:
                    raise
                result = default
                break
            except WMIBaseError:
                if queries <= retries:
                    self.callback.info('retrying')
                    self._sleep()
                    continue
                raise
        if result:
            return result.output
        return result

    def setup(self):
        """
        Create any keys that are not created by rTIS or are needed before
        rTIS is installed.
        """

        try:
            self._query(self._wmi.registryGetKey, self._conary_keypath,
                'system_model', raiseErrors=True)
        except WMIFileNotFoundError:
            self.callback.info('Creating Required Registry Keys')
            self._wmi.registryCreateKey('SOFTWARE', 'rPath')
            self._wmi.registryCreateKey(r'SOFTWARE\rPath', 'rTIS.NET')
            self._wmi.registryCreateKey(r'SOFTWARE\rPath\rTIS.NET', 'conary')
            self.system_model = ''
            self.polling_manifest = ''

    @_filter
    def _get_system_model(self):
        self.callback.info('Retrieving current system model')
        result = self._query(self._wmi.registryGetKey, self._conary_keypath,
            'system_model', default=[])
        return result

    def _set_system_model(self, model):
        self.callback.info('Writing system model')
        self._query(self._wmi.registrySetKey, self._conary_keypath,
            'system_model', model)

    system_model = property(_get_system_model, _set_system_model)

    @_filter
    def _get_manifest(self):
        self.callback.info('Retrieving current system manifest')

        lines = []
        if self._smb.pathexists(self.updatesDir, '..', 'manifest'):
            fh = self._smb.pathopen(self.updatesDir, '..', 'manifest', mode='r',
                codec='utf-8-sig')

            lines = [ x.strip() for x in fh ]
            fh.close()

        return [ TroveTuple(x) for x in lines ]

    def _set_manifest(self, data):
        self.callback.info('Writing system manifest')
        data = [ x.asString(withTimestamp=True) for x in data ]

        fh = self._smb.pathopen(self.updatesDir, '..', 'manifest', mode='w',
            codec='utf-8-sig')

        for line in data:
            fh.write('%s\r\n' % line)
        fh.close()

    manifest = property(_get_manifest, _set_manifest)

    @_filter
    def _get_polling_manifest(self):
        self.callback.info('Retrieving polling manifest')
        result = self._query(self._wmi.registryGetKey, self._conary_keypath,
            'polling_manifest', default=[])
        return result

    def _set_polling_manifest(self, data):
        self.callback.info('Writing polling manifest')
        self._query(self._wmi.registrySetKey, self._conary_keypath,
            'polling_manifest', data)

    polling_manifest = property(_get_polling_manifest, _set_polling_manifest)

    @_filter
    def _get_commands(self):
        self.callback.info('Retrieving commands')
        result = self._query(self._wmi.registryGetKey, self._params_keypath,
            'Commands', default=[])
        return result

    def _set_commands(self, data):
        self.callback.info('Setting commands %s' % data)
        self._query(self._wmi.registrySetKey, self._params_keypath,
            'Commands', data)

    commands = property(_get_commands, _set_commands)

    def _get_runcount(self):
        self.callback.info('Retrieving runcount')
        result = self._query(self._wmi.registryGetKey, self._params_keypath,
            'Runcount', default='')
        assert len(result) == 1
        return result[0]

    def _set_runcount(self, data):
        self.callback.info('Setting runcount %s' % data)
        self._query(self._wmi.registrySetKey, self._params_keypath,
            'Runcount', data)

    runcount = property(_get_runcount, _set_runcount)

    @property
    def flavor(self):
        """
        Query the remote system to determine the system flavor.
        """

        if self._flavor is not None:
            return self._flavor

        self.callback.info('Determinig System Flavor')
        result = self._query(
            self._wmi.registryGetKey,
            r'SYSTEM\CurrentControlSet\Control\Session Manager\Environment',
            'PROCESSOR_ARCHITECTURE',
        )

        arch = result[0]
        if arch == 'AMD64':
            self._flavor = deps.parseFlavor('is: x86 x86_64')
        else:
            self._flavor = deps.parseFlavor('is: x86')

        return self._flavor

    @property
    def updatesDir(self):
        if self._updatesDir:
            return self._updatesDir

        self.callback.info('Determining updates directory')
        result = self._query(
            self._wmi.registryGetKey,
            r'SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders',
            'Common AppData'
        )

        updatesDir = result[0]
        updatesDir = self._smb.getUnixPath(updatesDir)
        self._updatesDir = self._smb.pathjoin(updatesDir, 'rPath', 'Updates')

        return self._updatesDir

    @property
    def isInstalled(self):
        """
        Check to see if rTIS is installed on the target machine.
        """

        res = self._query(self._wmi.registryGetKey,
            self._params_keypath, 'Running', retries=0)
        return bool(res)

    @property
    def hasRun(self):
        """
        Report if rTIS has been run.
        """

        runcount = self.runcount
        if self._runcount is not None and runcount != self._runcount:
            return True
        else:
            self._runcount = runcount
            return False

    def resetRunStatus(self):
        """
        Reset the has run flag.
        """

        self._runcount = None

    def _reportStatus(self, logPath):
        """
        Report the last like of the logfile in the given path.
        """

        if not self._smb.pathexists(logPath, Servicing.LOGFILE):
            return

        line = None
        fh = self._smb.pathopen(logPath, Servicing.LOGFILE)
        for line in fh: pass
        fh.close()

        # Empty file.
        if line is None:
            return

        # Trim timestamp from line since we log timestamps further
        # up the stack.
        line = line.strip().split()
        if len(line) > 3:
            line = line[3:]
        line = ' '.join(line)

        self.callback.info(line)

    def start(self):
        """
        Start the rTIS service.
        """

        # Starting a new run, reset the internal status flag.
        self.resetRunStatus()

        # Cache the run status
        self.hasRun

        # Start the service.
        self._query(self._wmi.serviceStart, self._service_name)  # pyflakes=ignore

        # Now wait for the service to actually start.
        state = None
        statusKey = 'Running'
        start = time.time()
        while state != 'running' and not self.hasRun:
            try:
                result = self._query(self._wmi.registryGetKey,
                                     self._params_keypath,
                                     statusKey, raiseErrors=True)
                if result:
                    state = result[0]

            # FIXME: There should be a better way
            # If we are updating rTIS as the first job, the key that we are
            # polling could go away. Ignore missing keys. Yes this means that
            # we could just end up waiting for 30s if there is an error, but
            # it's better than failing.
            except WMIFileNotFoundError:
                pass

            if time.time() - start > 30:
                raise (ServiceFailedToStartError, 'The rPath Tools Installer '
                    'service failed to start. %s')

    def wait(self, allowReboot=True, reportStatus=None):
        """
        Wait for the install service to become available.
        """

        # Don't wait for the service if it
        # is not installed.
        if not self.isInstalled:
            return

        # If the service has already run, don't wait for it.
        if self.hasRun:
            return

        self.callback.info('Waiting for the %s to exit' % self._service_name)

        rebootStartTime = 0

        statusKey = 'Running'

        status = None
        while status != 'stopped':
            try:
                res = self._query(self._wmi.registryGetKey,
                                  self._params_keypath,
                                  statusKey, raiseErrors=True)
                status = res[0]
            except WMIBaseError:
                # Handle reboot case
                # NOTE: This may not actually be a reboot, the system may just
                #       not be responding for some amount of time.
                if allowReboot and not rebootStartTime:
                    rebootStartTime = time.time()
                    self.callback.info('Waiting for remote system to respond')

                # Handle reboot error case.
                elif (rebootStartTime and
                      time.time() - rebootStartTime > self._reboot_timeout):

                    self.callback.error('Unable to contact remote system')
                    raise

                # Raise any remaining errors.
                else:
                    raise

            if status == "running":
                rebootStartTime = 0
            elif status == "rebooting":
                rebootStartTime = 0
                self.callback.info('Reboot successfull, waiting for software '
                    'installation to complete')

            if reportStatus:
                self._reportStatus(reportStatus)

            self._sleep()

    def applyCriticalUpdate(self, updJob):
        """
        Install the version of rTIS that is included in the update job using
        msiexec.
        """

        criticalJob = updJob.getCriticalJob()

        if not criticalJob:
            return

        # Write system model
        # overwrite the existing system model since it shouldn't exist yet.
        self.system_model = criticalJob.system_model

        logPath = self._smb.pathjoin('Windows', 'Temp', 'rpath_install.log')
        winLogPath = self._smb.getWindowsPath('Windows/Temp/rpath_install.log')
        msiexec = r'msiexec.exe /i %%s /quiet /l*vx %s' % winLogPath

        manifest = dict((x.name, x) for x in criticalJob.manifest)

        # Install rTIS
        result = None
        contents = criticalJob.getFileContents()
        for job in criticalJob:
            name, _, (version, flavor), _ = job

            manifest[name] = TroveTuple(name, version, flavor)

            # Skip over packages
            if ':' not in name:
                continue

            # lookup the file information
            f = contents[(name, version, flavor)]

            # open the remote file.
            localPath = 'Windows/Temp/%s' % f.name
            remotePath = self._smb.getWindowsPath(localPath)
            remote = self._smb.pathopen(localPath, mode='w')

            # get a file copy callback
            cb = self.callback.copyfile(
                'Copying %s=%s[%s]' % (name, version, flavor),
                f.info.contents.size(),
            )

            # actually copy the file
            util.copyfileobj(f.content, remote, callback=cb)
            remote.close()

            self.callback.info('installing %s=%s[%s]' % (name, version, flavor))
            result = self._wmi.processCreate(msiexec % remotePath)

        self.callback.info('waiting for installation to start')
        while not self._smb.pathexists(logPath):
            time.sleep(1)

        # Wait for MSI to complete the update
        self.callback.info('waiting for installation to complete')
        rc = None
        done = False
        magic = 'MainEngineThread is returning'
        while not done:
            fh = self._smb.pathopen(logPath, codec='utf_16_le')
            for line in fh:
                if magic in line:
                    done = True
                    parts = line[line.find(magic)+len(magic):].split()
                    if len(parts) > 0 and parts[0].isdigit():
                        rc = int(parts[0])
                    else:
                        rc = -1
                    break
            fh.close()
            time.sleep(1)

        if rc not in self._success:
            self.callback.error('msiexec failed to install rPathTools with the '
                'following error code: %s' % rc)
            raise MSIInstallationError

        self.callback.info('critical update installation complete')

        # Wait for PromgraData\rPath\manifest to exist before setting the
        # manifest, it should be created by rPathTools Setup.
        while not self._smb.pathexists(self.updatesDir, '..', 'manifest'):
            time.sleep(1)

        self.manifest = manifest.values()
        self.polling_manifest = criticalJob.polling_manifest

        return result

    def applyUpdate(self, updJob):
        """
        Coordinate with rTIS on the remote machine to install updates.
        """

        # If there are no updates in the update job, don't bother trying
        # to apply.
        if len(updJob) == 0:
            return []

        # Set the remote system model to match the desired state.
        self.system_model = updJob.system_model

        # download updates
        contents = updJob.getFileContents()

        servicing = Servicing.createUpdateJob()

        jobDir = self._smb.pathjoin(self.updatesDir, updJob.jobId)
        self._smb.mkdir(jobDir)

        # copy contents to the remote machine
        for job in updJob:
            name, (oldVer, oldFlv), (newVer, newFlv), _ = job

            f = contents.get((name, newVer, newFlv),
                contents.get((name, oldVer, oldFlv)))

            if oldVer is not None:
                oldNVF = TroveTuple(name, oldVer, oldFlv)
            else:
                oldNVF = None

            servicing.addPackage(f, oldNVF)

            if f.msi:
                pkgDir = self._smb.pathjoin(jobDir, f.msi.productCode())
                self._smb.mkdir(pkgDir)

            if not f.content:
                continue

            # Make sure there is enough available space to store the MSI plus
            # some overhead.
            stat = os.statvfs(pkgDir)
            fsSize = stat[statvfs.F_BFREE] * stat[statvfs.F_BSIZE]
            fsize = f.info.contents.size()

            if fsSize < fsize * 3:
                raise NotEnoughSpaceError, ('Not enough space on target '
                    'system to install %s' % f.nvf.asString())

            # get a file copy callback
            cb = self.callback.copyfile(
                'Copying %s' % f.nvf.asString(),
                f.info.contents.size(),
            )

            # open remote file.
            remote = self._smb.pathopen(pkgDir, f.name, mode='w')

            # actually copy the file
            util.copyfileobj(f.content, remote, callback=cb)
            remote.close()

        # Write out the servicing xml for this job.
        self.callback.debug(servicing.tostring(prettyPrint=True))
        fh = self._smb.pathopen(jobDir, servicing.FILENAME, mode='w')
        fh.write(servicing.tostring())
        fh.flush()
        fh.close()
        # REALLY CLOSE THE FILE DESCRIPTOR, I MEAN IT!
        try:
            os.close(fh.fileno())
        except:
            pass

        # Set rTIS to use the job directory that we just created.
        self.commands = updJob.jobId

        # Start rTIS
        self.start()

        # Wait for the service to complete the update job.
        self.wait(allowReboot=True, reportStatus=jobDir)

        # Parse results
        self.callback.info('Parsing Results')
        fh = self._smb.pathopen(jobDir, servicing.FILENAME)
        results = [ x for x in servicing.iterpackageresults(fh) ]
        self.callback.info('applyupdate read %s' % fh.fileno())
        fh.close()

        # write this at the end, after all updates have completed successfully.
        self.polling_manifest = updJob.polling_manifest

        # get return code
        self.callback.info('Cleaning up')
        rc = max([ int(x[2].get('exitCode')) for x in results
            if x[2].get('exitCode') is not None ] + [0, ])
        if rc == 0:
            self._smb.rmdir(jobDir)

        return results

    def applyConfiguration(self, jobId, values):
        """
        Configure and run rTIS to set configuration values on the target system.
        """

        jobId = 'job-%s' % jobId
        servicing = Servicing.createConfigJob()

        for value in values:
            servicing.addValue(value)

        jobDir = self._smb.pathjoin(self.updatesDir, jobId)
        self._smb.mkdir(jobDir)

        # Write out the servicing xml for this job.
        self.callback.debug(servicing.tostring(prettyPrint=True))
        fh = self._smb.pathopen(jobDir, servicing.FILENAME, mode='w')
        fh.write(servicing.tostring())
        self.callback.info('configure write %s' % fh.fileno())
        fh.close()

        # Set rTIS to use the job directory that we just created.
        self.commands = jobId

        # Start rTIS
        self.start()

        # Wait for the service to complete the update job.
        self.wait(allowReboot=True)

        # Get results from the target system
        fh = self._smb.pathopen(jobDir, servicing.FILENAME)
        results = [ x for x in servicing.iterconfigresults(fh)]
        self.callback.info('configure read %s' % fh.fileno())
        fh.close()

        return results

    def _queryPackages(self, jobId):
        """
        Query the list of packages from a remote Windows machine.
        """

        self.callback.info('Querying Windows system for package information.')

        if not jobId.startswith('job-'):
            jobId = 'job-%s' % jobId

        # Create directory for the results.
        jobDir = self._smb.pathjoin(self.updatesDir, jobId)
        self._smb.mkdir(jobDir)

        # Set the command.
        self.commands = 'query=%s' % jobId

        # Querying for packages is sometimes too fast for us to notice that the
        # service actually ran. Ignore the error and look for the output file.
        # If it isn't there, raise an error.

        try:
            # Start the service.
            self.start()
        except ServiceFailedToStartError:
            pass

        # Wait for the service to exit.
        self.wait()

        # Gather the results.
        if self._smb.pathexists(jobDir, Packages.FILENAME):
            fh = self._smb.pathopen(jobDir, Packages.FILENAME)
            packages = Packages.parse(fh)
            fh.close()
        else:
            raise ServiceFailedToStartError

        return packages
