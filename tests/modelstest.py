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
        self.failUnlessEqual(files.toXml(),
            '<files><file><title>i1</title><size>1</size><sha1>s1</sha1></file><file><title>i2</title><sha1>s2</sha1></file></files>')

    def testCDATASection(self):
        X = models.XML
        sub1 = X.Element('embedded', X.CDATA('data', '<xml/>'))
        x = X.Element('root', X.CDATA('sub', X.toString(sub1)))
        self.assertEquals(X.toString(x), '<root><sub><![CDATA[<embedded><data><![CDATA[<xml/>]]]]><![CDATA[></data></embedded>]]></sub></root>')

testsuite.main()
