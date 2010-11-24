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

class Codes(object):
    # 100
    MSG_START = 101
    MSG_CALL = 102
    MSG_NEW_TASK = 103
    MSG_REGISTRATION_REQ = 104
    MSG_CREDENTIALS_VALIDATION = 105
    MSG_PROBE = 106
    MSG_GENERIC = 110

    # 200
    OK = 200
    OK_1 = 201

    # 400
    ERR_AUTHENTICATION = 401
    ERR_NOT_FOUND = 404
    ERR_METHOD_NOT_ALLOWED = 405
    ERR_ZONE_MISSING = 420
    ERR_BAD_ARGS = 421
    ERR_GENERIC = 430

class WmiCodes(object):
    ERR_TIMEOUT = 0x100
    ERR_FILE_NOT_FOUND = 0x200
    ERR_ACCESS_DENIED = 0x500
    ERR_ACCESS_DENIED2 = 0xBD00
    ERR_BAD_CREDENTIALS = 0x6D00

    errMsg = {
        ERR_TIMEOUT: "Timeout waiting for a response",
        ERR_FILE_NOT_FOUND: "The file or registry key/value pair cannot be found.",
        ERR_ACCESS_DENIED: "The credentials provided do not have permission to access the requested resource. If this system is running Windows 2008 R2, please refer to the 'rPath Platform Guilde for Microsoft Windows' for special configuration requirements necessary to enable remote WMI access.",
        ERR_ACCESS_DENIED2: "The credentials provided do not have permission to access the requested resource.",
        ERR_BAD_CREDENTIALS: "The username, password or domain is invalid"
        }

    @classmethod
    def errorMessage(cls, errCode, returnText, message="", params={}):
        if message:
            m = message
        else:
            m = ""

        m = m + cls.errMsg.get(errCode, "Undefined Error Code")
        m = m + '\n\nWMIClient Error Code: ' + str(errCode)
        if params:
            m = m + '\n\nAdditional Details:'
        for p in params.items():
            m = m + '\n%s = %s' % (str(p[0]), str(p[1]))
        m = m + "\n\nInformation Returned from the client:\n" + returnText

        return m
