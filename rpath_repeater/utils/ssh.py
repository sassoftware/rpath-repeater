# Copyright (c) 2011 rPath, Inc.
# SSH node communication tools

import paramiko

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
                 key=None, clientClass=paramiko.SSHClient, 
                 sftpClass=paramiko.SFTPClient):
       self.host        = host
       self.port        = port
       self.user        = user
       self.password    = password
       self.key         = key
       self.clientClass = clientClass
       self.sftpClass   = sftpClass
       self.client      = self._genClient()

    def _genClient(self):
       '''
       Get a SSHClient handle, allows auth by key or username/password
       '''
       client = self.clientClass()
       client.load_system_host_keys()
       # might want an 'ignore' policy that doesn't chirp to stderr later
       client.set_missing_host_key_policy(paramiko.WarningPolicy())
       if self.key and self.key != '':
           # try the ssh key, password protected keys are ok
           try:
               client.connect(self.host, port=self.port,
                   password=self.password, key_filename=self.key,
                   allow_agent=True) # look_for_keys=True)
           except paramiko.PasswordRequiredException:
               # this won't retry the unlock password as your username/password
               raise Exception("invalid key password")
       else:
           # no key provided, try username/password
           client.connect(self.host, port=self.port, username=self.user,
               password=self.password, allow_agent=True)
               # look_for_keys=True)
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


