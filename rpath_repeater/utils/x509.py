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


"Simple module for generating x509 certificates"

import os
import tempfile

from rmake3.lib import gencert

class X509(object):
    class Options(object):
        __slots__ = ['C', 'ST', 'L', 'O', 'OU', 'CN', 'site_user',
                     'key_length', 'expiry', 'output', 'output_key']
        _defaults = dict(key_length = 2048, expiry = 3 * 365)
        def __init__(self, **kwargs):
            params = self._defaults.copy()
            params.update(kwargs)
            # Initialize from slots
            for slot in self.__slots__:
                val = params.get(slot, None)
                setattr(self, slot, val)

    @classmethod
    def new(cls, commonName, certDir):
        """
        Generate X509 certificate with the specified commonName
        Returns absolute paths to cert file and key file
        """

        fd, tempFile = tempfile.mkstemp(dir = certDir, prefix = 'new-cert-')
        os.close(fd)
        certFile = tempFile
        keyFile = certFile + '.key'

        o = cls.Options(CN = commonName, output = certFile,
            output_key = keyFile)
        gencert.new_ca(o, isCA = False)

        hash = cls.computeHash(certFile)
        newCertFile = os.path.join(certDir, hash + '.0')
        newKeyFile = os.path.join(certDir, hash + '.0.key')
        os.rename(certFile, newCertFile)
        os.rename(keyFile, newKeyFile)
        return newCertFile, newKeyFile

    @classmethod
    def load(cls, certFile):
        x509 = gencert.X509.load_cert(certFile)
        return x509

    @classmethod
    def computeHash(cls, certFile):
        x509 = cls.load(certFile)
        certHash = "%08x" % x509.get_issuer().as_hash()
        return certHash
