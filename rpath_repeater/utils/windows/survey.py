#
# Copyright (c) 2011 rPath, Inc.
#

from lxml import etree
from lxml.builder import ElementMaker

class Survey(object):
    """
    Class for adding any additional information to a Windows survey.
    """

    def __init__(self, survey_data, rtis):
        # survey_data is a etree element
        self._data = survey_data
        self._rtis = rtis

        self.e = ElementMaker()

    def tostring(self, prettyPrint=False):
        root = self.e.surveys()
        root.append(self._data)
        return etree.tostring(root, pretty_print=prettyPrint)

    def addPackageInformation(self):
        """
        Add the conary package information to the survey.
        """

        # 1. get the conary package data from the target system
        # 2. get troveInfo for all packages from teh repository
        # 3. map conary packages to windows packages and vice versa

