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

import os

from conary.lib import cfg

class repeaterConfiguration(cfg.ConfigFile):
    httpPort = (cfg.CfgInt, 8080)
    httpsPort = (cfg.CfgInt, 8443)
    #credentialPath = (cfg.CfgString, "/etc/conary/rMN/endpoint.cred")
    credentialPath = (cfg.CfgString, "/tmp/endpoint.cred")
    xmppUsername = (cfg.CfgString, "sput")
    xmppPassword = (cfg.CfgString, "password")
    xmppDomain = (cfg.CfgString, "jabber.eng.rpath.com")
    neighbors = (cfg.CfgList(cfg.CfgString), ["rbuilder@jabber.eng.rpath.com/jabberlink",])
    repeaterHub = (cfg.CfgBool, 0)
    repeaterSpoke = (cfg.CfgBool, 1)
    repeaterTypes = (cfg.CfgList(cfg.CfgString), ["HTTP",])
    repeaterDestinations = (cfg.CfgList(cfg.CfgString), ["www.cnn.com",])
    logFile = (cfg.CfgString, '/var/log/repeater')
    debugMode = (cfg.CfgBool, False)
    
    def __init__(self, readConfigFiles=False, ignoreErrors=False, root=''):
        cfg.ConfigFile.__init__(self)
        if readConfigFiles:
            self.readFiles()

    def readFiles(self, root=''):
        """
        Populate this configuration object with data from all
        standard locations for rbuilder configuration files.
        @param root: if specified, search for config file under the given
        root instead of on the base system.  Useful for testing.
        """
        self.read(root + '/etc/conary/rMN/repeaterrc', exception=False)
        if os.environ.has_key("HOME"):
            self.read(root + os.environ["HOME"] + "/" + ".repeaterrc",
                      exception=False)
        self.read('repeaterrc', exception=False)
        
        