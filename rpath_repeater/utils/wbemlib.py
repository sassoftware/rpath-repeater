#!/usr/bin/python2.4
#
# Copyright (c) 2009 rPath, Inc.
#

import inspect
import pywbem

class WBEMException(Exception):
    '''
    Base Exception class.
    '''
    pass

class WBEMUnexpectedReturnException(WBEMException):
    def __init__(self, expectedReturnCode, returnCode, returnMsg, *args, **kwargs):
        self.expectedReturnCode = expectedReturnCode
        self.returnCode = returnCode
        self.returnMsg = returnMsg
        WBEMException.__init__(self, *args, **kwargs)

    def __str__(self):
        return "Expected return code %s, got %s: %s" % \
            (str(self.expectedReturnCode), str(self.returnCode), str(self.returnMsg))

class WBEMConnection(pywbem.WBEMConnection):
    '''
    Simple subclass with a single method to determine if we want to use a
    typical CIM method like GetInstance or call a custom method via
    InvokeMethod.
    '''
    invokeMethodUsed = False

    def callMethod(self, CIMClassName, methodName, *args, **kw):
        # We can either call methods defined on our CIM class, or predefined
        # methods like {Get,Enumerate}Instance which pywbem has wrapped for
        # us.  We'll look on self.conn to see if there is a wrapper for the
        # method first, then use InvokeMethod.
        if hasattr(self, methodName):
            func = getattr(pywbem.WBEMConnection, methodName)
            argSpec = inspect.getargspec(func)
            if len(argSpec[0]) > 0 and argSpec[0][1] == 'ClassName':
                args = (CIMClassName, ) + args
        else:
            # Need to make use of InvokeMethod, prepend CIMClassName and
            # methodName to the front of args.
            func = getattr(pywbem.WBEMConnection, 'InvokeMethod')
            if CIMClassName:
                args = (CIMClassName, ) + args
            args = (methodName,) + args
            self.invokeMethodUsed = True

        return func(self, *args, **kw)

class _CallableMethod(object):
    '''
    Class to implement a __call__ method so that we can call CIM methods in a
    pythonic way.
    '''

    def __init__(self, send, name):
        '''
        self._send: method to execute to do the actual remote invocation
        self._name: actual remote method to execute
        '''
        self._send = send
        self._name = name
        self._CIMClassName = None

    def __getattr__(self, attr):
        self._CIMClassName = self._name
        self._name = attr
        return self

    def __call__(self, *args, **kw):
        return self._send(self._CIMClassName, self._name, *args, **kw)

class WBEMServer(object):
    '''
    Abstract representation of a WBEM Server.

    Meant to work like xmlrpclib.ServerProxy so that you can use python's dot
    notation to call remote methods.

    You can call custom remote methods and methods that are definied for you
    already as instance methods on pywbem.WBEMConnection.  When you call a
    custom method, the 1st (MethodName) and 2nd (ObjectName) arguments to
    WBEMConnection.InvokeMethod are set for you based on the name of the
    attribute accessed and the current set value of CIMClassName,
    respectively.

    Example:
    >>> server = WBEMServer('http://localhost', x509 = dict(cert_file = "cert.crt", key_file = "cert.key"))
    >>> result = server.GetClass('MyCIMClassName')
    >>> result = server.CustomMethod(methodArg1, methodArg2)
    '''

    namespace = 'root/cimv2'

    def __init__(self, host, debug=False, **kwargs):
        self.host = host
        self.debug = debug
        # XXX FIXME: pywbem uses pyopenssl for the SSL verification callback,
        # and in a very awkward manner too: it will open an SSL socket to
        # which it passes the callback, then (if successful) close the socket
        # and re-open it using HTTPSConnection. This is dangerous (aside from
        # inefficient)
        self.conn = WBEMConnection(host, **kwargs)
        self.conn.default_namespace = self.namespace
        self.conn.debug = self.debug
        self.returnCodes = {}

    def __getattr__(self, attr):
        return _CallableMethod(self._method_call, attr)

    def _method_call(self, CIMClassName, methodName, *args, **kw):
        try:
            self.conn.invokeMethodUsed = False
            result = self.conn.callMethod(
                CIMClassName, methodName, *args, **kw)
        except pywbem.CIMError, e:
            raise WBEMException(str(e))

        return result

    def _normalizeValueMap(self, values, valueMap):
        valuesDict = {}
        valuePairs = [(valueMap.pop(), values.pop()) for x in xrange(len(values))]
        for k, v in valuePairs:
            valuesDict[k] = v
        return valuesDict

    def getMethodReturnCodes(self, className, methodName, force=False):
        if (className, methodName) not in self.returnCodes or force:
            cimClass = self.conn.GetClass(className)
            returnCodes = cimClass.methods[methodName].qualifiers

            # Turn returnCodes into key/value pairs of integers and
            # descriptions so that it's easy to work with.
            codes = self._normalizeValueMap(
                returnCodes['Values'].value, returnCodes['ValueMap'].value)

            self.returnCodes[(className, methodName)] = codes

        return self.returnCodes[(className, methodName)]

    def getError(self, job):
        returnCode, error = self.GetError(job.path)

        if returnCode != 0:
            raise WBEMUnexpectedReturnException(0, returnCode, 
                "Error calling GetError.")

        return error['Error'].properties['Message'].value              
