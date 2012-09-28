# Copyright (C) 2010 rPath, Inc.

import testsuite
testsuite.setup()

from testrunner import testcase

from lxml import etree

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
        if not hasattr(etree, "CDATA"):
            raise testcase.SkipTestException("CDATA not present in old lxml versions")
        X = models.XML
        sub1 = X.Element('embedded', X.CDATA('data', '<xml/>'))
        x = X.Element('root', X.CDATA('sub', X.toString(sub1)))
        self.assertEquals(X.toString(x), '<root><sub><![CDATA[<embedded><data><![CDATA[<xml/>]]]]><![CDATA[></data></embedded>]]></sub></root>')

    def testScriptOutput(self):
        if not hasattr(etree, "CDATA"):
            raise testcase.SkipTestException("CDATA not present in old lxml versions")
        so = models.ScriptOutput(statusCode=10, stdout=" <blah/>\n", stderr="some data")
        xml = so.toXml()
        self.assertEquals(xml,
            '<scriptOutput><stdout><![CDATA[ <blah/>\n]]></stdout><stderr>some data</stderr></scriptOutput>')

testsuite.main()
