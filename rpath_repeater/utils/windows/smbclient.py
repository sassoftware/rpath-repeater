#
# Copyright (c) rPath, Inc.
#

"""
Basic abstraction around coordinating smb mounts and copying files to a
Windows system.
"""

import logging
import os
import time
import codecs
import shutil
import tempfile
import subprocess

from conary.lib import util

from rpath_repeater.utils.windows import errors
from rpath_repeater.utils.windows.callbacks import BaseCallback

log = logging.getLogger('windows.smbclient')


class SMBClientError(errors.BaseException):
    pass

class SMBMountError(SMBClientError):
    pass


class SMBClient(object):
    # These are the possible return codes according to the mount man page.
    _mount_rc = {
       0: 'success',
       1: 'incorrect invocation or permissions',
       2: 'system error (out of memory, cannot fork, no more loop devices)',
       4: 'internal mount bug or missing nfs support in mount',
       8: 'user interrupt',
       16: 'problems writing or locking /etc/mtab',
       32: 'mount failure',
       64: 'some mount succeeded',
    }

    def __init__(self, authInfo, driveLetter='C', callback=None):
        self._authInfo = authInfo
        self._driveLetter = driveLetter

        if not callback:
            self.callback = BaseCallback()
        else:
            self.callback = callback

        self._rootdir = None
        self._mounted = False

        # Older mount.cifs don't seem to support passing the user via an
        # environment variable
        self._mount_cmd = [ '/bin/mount', '-n', '-t', 'cifs', '-o',
            'user=%(user)s,password=%(password)s,forcedirectio',
            '//%%(host)s/%s$' % self._driveLetter ]
        self._mount_env = dict(PASSWD=self._authInfo.password)

        self._umount_cmd = [ '/bin/umount', ]

    def close(self):
        if self._mounted:
            self._umount()

    def _runCmd(self, cmd, env=None, rcMap=None):
        if not rcMap:
            rcMap = {}

        info = self._authInfo._asdict()
        cmd = [ x % info for x in cmd ]

        p = subprocess.Popen(cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env)

        rc = None
        while True:
            rc = p.poll()
            if rc is not None:
                break

            time.sleep(1)

        if rc is not None and rc != 0:
            raise SMBMountError, ('SMB mount failed: %s, stdout: %s, stderr: %s'
                % (rcMap.get(rc, ''), p.stdout.read(), p.stderr.read()))

    def _mount(self):
        if self._rootdir:
            return

        log.info('mounting windows share')

        error = None
        retries = 10
        self._rootdir = tempfile.mkdtemp()
        while retries:
            try:
                self._runCmd(self._mount_cmd + [ self._rootdir, ],
                    self._mount_env, self._mount_rc)
                break
            except SMBMountError, e:
                time.sleep(3)
                retries -= 1
                error = e
            except Exception:
                retries = None
                self._rootdir = None
                raise

        if retries <= 0 and error:
            raise error

        self._mounted = True

    def _umount(self):
        if not self._rootdir:
            return

        log.info('unmounting windows share')

        try:
            self._runCmd(self._umount_cmd + [ self._rootdir, ])
        finally:
            self._rootdir = None
            self._mounted = False

    def pathjoin(self, *paths, **kwargs):
        if not self._mounted:
            self._mount()

        paths = [ os.path.normpath(x) for x in paths ]

        if kwargs.get('relpath', False):
            root = ''
        else:
            root = os.path.normpath(self._rootdir)

        if paths[0].startswith(root) or root == os.sep:
            root = ''
            if paths[0].startswith('/'):
                paths[0] = paths[0][1:]

        for path in paths:
            root += os.sep + os.path.normpath(path)
        return os.path.abspath(root)

    def pathexists(self, *paths):
        return os.path.exists(self.pathjoin(*paths))

    def mkdir(self, *paths):
        path = self.pathjoin(*paths)
        if not os.path.exists(path):
            os.makedirs(path)

    def rmdir(self, *paths):
        path = self.pathjoin(*paths)
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)

    def pathopen(self, *paths, **kwargs):
        mode = kwargs.get('mode', 'r')
        codec = kwargs.get('codec', None)
        path = self.pathjoin(*paths)

        if not os.path.exists(os.path.dirname(path)):
            util.mkdirChain(os.path.dirname(path))

        if codec:
            return codecs.open(path, mode, codec)
        else:
            return open(path, mode)

    def getWindowsPath(self, *paths):
        relpath = self.pathjoin(*paths, relpath=True)
        path = self._driveLetter + ':' + relpath.replace('/', '\\')
        return path

    def getUnixPath(self, *paths):
        assert len(paths) == 1
        path = paths[0]
        path = path.replace('\\', '/')
        if path.startswith(self._driveLetter):
            path = path[3:]
        return self.pathjoin(path)
