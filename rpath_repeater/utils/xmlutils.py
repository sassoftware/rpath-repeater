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


from lxml import etree

class XML(object):
    @classmethod
    def Text(cls, tagName, text):
        node = etree.Element(tagName)
        node.text = unicode(text)
        return node

    @classmethod
    def CDATA(cls, tagName, text):
        CDATA = getattr(etree, 'CDATA', None)
        if CDATA is None:
            return cls.Text(tagName, text)
        node = etree.Element(tagName)
        node.text = CDATA(unicode(text))
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
