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


"""
SSH node communication tools
"""


import paramiko
import logging
from rpath_repeater.codes import Codes as C

class PrivateKey(object):
    """
    A file-like object for use w/ paramiko
    """

    def __init__(self, key_bytes):
        self.key_bytes = key_bytes

    def read(self):
        return self.key_bytes

    def readlines(self):
        return self.key_bytes.split("\n")

class SshConnector(object):

    """
    Paramiko SSH connection, adapted to our own validation needs.
    Heavily based off the paramiko demo directory version, but non-interactive.

    Usage: 
        sconn = SshConnector(host='foo.example.org',...)
        code, output = sconn.execCommand('...')
        sconn.getFile(remote,local) # or putFile(local,remote)
        sconn.close()

    This module does not check known_hosts (or add machienes to known hosts)
    because it assumes machines will be frequently reprovisioned.
    """

    def __init__(self, host=None, port=22, user='root', password='password', 
                 key=None, status=None, clientClass=paramiko.SSHClient, 
                 sftpClass=paramiko.SFTPClient):
       ''' 
       Represents one attempt to connect to a system.
       Password is only used if sshKey is not provided or sshKey is locked
       in which case it acts as a password unlock key.

       Alternative ssh client classes can be passed in for testing.
       '''
       self.host        = host
       self.port        = port
       self.user        = user
       self.password    = password
       self.key         = key
       self.clientClass = clientClass
       self.sftpClass   = sftpClass
       self._status    = status 
       if self.user is None:
           self.user = 'root'
       self.client      = self._genClient()
      
    def status(self, code, msg):
       if self._status:
           self._status(code, msg)

    def _genClient(self):
       '''
       Get a SSHClient handle, allows auth by key or username/password
       '''
       client = self.clientClass()
       # try to avoid stdout spewage from Paramiko

       logger = paramiko.util.logging.getLogger()
       logger.setLevel(logging.CRITICAL)

       client.set_log_channel(None)

       if self.password == '':
           self.password = None

       # might want an 'ignore' policy that doesn't chirp to stderr later
       client.set_missing_host_key_policy(paramiko.WarningPolicy())
       if self.key and self.key != '':

           key_obj = PrivateKey(self.key)
           self.key = None
           try:
               self.key = paramiko.DSSKey.from_private_key(file_obj=key_obj, password=self.password)
           except paramiko.SSHException: 
               self.key = paramiko.RSAKey.from_private_key(file_obj=key_obj, password=self.password)

           # try the ssh key, password protected keys are ok
           try:
               self.status(C.MSG_GENERIC, 'attempting SSH login with key')
               client.connect(
                   self.host, 
                   port=self.port,
                   username=self.user,
                   password=self.password,
                   look_for_keys=False,
                   pkey=self.key,
                   allow_agent=False
               ) 
           except paramiko.PasswordRequiredException:
               # this won't retry the unlock password as your username/password
               raise Exception("invalid key password")
       else:
           # no key provided, try username/password
           self.status(C.MSG_GENERIC, 'attempting SSH login with password')
           client.connect(
               self.host, 
               port=self.port, 
               username=self.user,
               password=self.password, 
               allow_agent=False, 
               look_for_keys=False
           )
       self.status(C.MSG_GENERIC, 'connection established')
       return client

    def close(self):
        '''SSH disconnect'''
        self.client.close()

    def execCommand(self, cmd):
        '''Runs a non-interactive command and returns both the exit code & output'''
        cmd = cmd + "; echo $?"
        sin, sout, serr = self.client.exec_command(cmd)
        results = sout.read()
        lines = results.split("\n")
        status = int(lines[-2])
        results = results[0:-2].strip()
        return (status, results)

    def _sftp(self):
        '''Create a SFTP connection'''
        return self.sftpClass.from_transport(self.client.get_transport())

    def putFile(self, localFile, remoteFile):
        '''place a file on the remote system'''
        sftp = self._sftp()
        sftp.put(localFile, remoteFile)
        sftp.close()

    def getFile(self, remoteFile, localFile):
        '''download a remote file'''
        sftp = self._sftp()
        sftp.get(remoteFile, localFile)
        sftp.close()

    def unlink(self, remoteFile):
        '''delete a remote file'''
        sftp = self._sftp()
        sftp.unlink(remoteFile)
        sftp.close()
