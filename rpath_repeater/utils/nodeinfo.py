#
# Copyright (c) 2009 rPath, Inc.  All Rights Reserved.
#

import socket
import fcntl
import struct

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

