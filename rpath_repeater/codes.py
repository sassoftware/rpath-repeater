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
    CIM_TASK_SURVEY_SCAN = '%s.survey.scan' % CIM_JOB

    WMI_JOB = "%s.wmiplugin" % PREFIX
    WMI_TASK_REGISTER = '%s.register' % WMI_JOB
    WMI_TASK_SHUTDOWN = '%s.shutdown' % WMI_JOB
    WMI_TASK_POLLING = '%s.poll' % WMI_JOB
    WMI_TASK_UPDATE = '%s.update' % WMI_JOB
    WMI_TASK_CONFIGURATION = '%s.configuration' % WMI_JOB
    WMI_TASK_SURVEY_SCAN = '%s.survey.scan' % WMI_JOB
