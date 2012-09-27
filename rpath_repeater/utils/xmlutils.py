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

from lxml import etree

class XML(object):
    @classmethod
    def Text(cls, tagName, text):
        node = etree.Element(tagName)
        node.text = text
        return node

    @classmethod
    def CDATA(cls, tagName, text):
        node = etree.Element(tagName)
        node.text = etree.CDATA(text)
        return node

    @classmethod
    def Element(cls, tagName, *children, **attributes):
        node = etree.Element(tagName,
            dict((k, unicode(v)) for k, v in attributes.items()))
        node.extend(children)
        return node

    @classmethod
    def toString(cls, elt):
        return etree.tostring(elt, encoding='UTF-8')

    @classmethod
    def fromString(cls, strng):
        return etree.fromstring(strng)
