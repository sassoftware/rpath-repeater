# Copyright (c) 2011 rPath, Inc.
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
    MSG_PROGRESS = 112

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

    # 800 - partial results
    PART_RESULT_1 = 801
    PART_RESULT_2 = 802
    PART_RESULT_3 = 803
    PART_RESULT_4 = 804

class NS(object):
    PREFIX = "com.rpath.sputnik"

    TARGET = "%s.targetsplugin" % PREFIX
    TARGET_TEST_CREATE = "%s.test.create" % TARGET
    TARGET_TEST_CREDENTIALS = "%s.test.credentials" % TARGET
    TARGET_IMAGES_LIST = "%s.images.list" % TARGET
    TARGET_INSTANCES_LIST = "%s.instances.list" % TARGET
    TARGET_SYSTEM_CAPTURE = "%s.instances.capture" % TARGET
    TARGET_IMAGE_DEPLOY = "%s.images.deploy" % TARGET
    TARGET_IMAGE_DEPLOY_DESCRIPTOR = "%s.images.deploy.descriptor" % TARGET
    TARGET_SYSTEM_LAUNCH = "%s.instances.launch" % TARGET
    TARGET_SYSTEM_LAUNCH_DESCRIPTOR = "%s.instances.launch.descriptor" % TARGET

    CIM_JOB = "%s.cimplugin" % PREFIX
    CIM_TASK_REGISTER = '%s.register' % CIM_JOB
    CIM_TASK_SHUTDOWN = '%s.shutdown' % CIM_JOB
    CIM_TASK_POLLING = '%s.poll' % CIM_JOB
    CIM_TASK_UPDATE = '%s.update' % CIM_JOB
    CIM_TASK_CONFIGURATION = '%s.configuration' % CIM_JOB

    WMI_JOB = "%s.wmiplugin" % PREFIX
    WMI_TASK_REGISTER = '%s.register' % WMI_JOB
    WMI_TASK_SHUTDOWN = '%s.shutdown' % WMI_JOB
    WMI_TASK_POLLING = '%s.poll' % WMI_JOB
    WMI_TASK_UPDATE = '%s.update' % WMI_JOB
    WMI_TASK_CONFIGURATION = '%s.configuration' % WMI_JOB
