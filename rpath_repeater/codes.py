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
    MSG_BOOTSTRAP_REQ = 111

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
