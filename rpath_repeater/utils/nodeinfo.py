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


import sys
import fcntl
import select
import socket
import struct

from OpenSSL import SSL
from OpenSSL import crypto

class ProbeHostError(Exception):
    pass

def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])

def get_hostname():
    return socket.getfqdn()

def probe_host(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(10)
    try:
        s.connect((host, port))
    except socket.error, e:
        raise ProbeHostError(str(e))
    s.close()
    return True

def probe_host_ssl(host, port, cert_file=None, key_file=None,
        ssl_server_cert=None):
    """
    Probe the given host for an SSL connection on the given port.
    The optional cert_file and key_file arguments point to a client-side
    certificate pair to be used.
    If sslServerCert is provided, the server's certificate will be verified
    against this one.
    If successful, the function returns the server's SSL certificate in PEM
    format. Otherwise, None is returned.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Setting the timeout here (for the sake of not blocking if packets are
    # dropped) complicates do_handshake.
    sock.settimeout(10)
    try:
        sock.connect((host, port))
    except socket.error, e:
        ei = sys.exc_info()
        raise ProbeHostError(str(e)), None, ei[2]

    ctx = SSL.Context(SSL.SSLv23_METHOD)
    ctx.set_options(SSL.OP_NO_SSLv2)
    if cert_file:
        ctx.use_certificate_file(cert_file)
    if key_file:
        ctx.use_privatekey_file(key_file)
    if 0 and ssl_server_cert:
        # for some unknown reason, this does not work
        ctx.load_verify_locations(ssl_server_cert)
        ctx.set_verify_depth(5)
        ctx.set_verify(SSL.VERIFY_PEER, verifyCallback)


    conn = SSL.Connection(ctx, sock)
    conn.set_connect_state()

    pollObj = select.poll()
    pollObj.register(sock, select.POLLIN)

    for i in range(100):
        try:
            conn.do_handshake()
            break
        except SSL.WantReadError:
            while not pollObj.poll(100):
                pass
        except SSL.Error:
            ei = sys.exc_info()
            raise ProbeHostError, ei[1], ei[2]
    cert = conn.get_peer_certificate()
    certPem = crypto.dump_certificate(SSL.FILETYPE_PEM, cert)
    conn.close()
    sock.close()
    return certPem

def verifyCallback(conn, x509, errno, depth, retcode):
    return retcode
