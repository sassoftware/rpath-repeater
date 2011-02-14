# Copyright (C) 2010 rPath, Inc.

import testsuite
testsuite.setup()

from testrunner import testcase

from rpath_repeater import models

class TestBase(testcase.TestCaseWithWorkDir):
    pass

class ModelsTest(TestBase):
    def testModelToXml(self):
        files = models.ImageFiles([
            models.ImageFile(title="i1", sha1="s1", size=1),
            models.ImageFile(title="i2", sha1="s2"),
        ])
        metadata = models.ImageMetadata(owner="me")
        files.append(metadata)
        self.failUnlessEqual(files.toXml(),
            '<files><file><title>i1</title><size>1</size><sha1>s1</sha1></file><file><title>i2</title><sha1>s2</sha1></file><metadata><owner>me</owner></metadata></files>')

testsuite.main()
