# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

import errno
import os
import socket
import stat
import sys
import time
import logging

from thriftsvr import util

class BaseSocket(object):

    def __init__(self, address):
        logging.info("------------BaskSocket::init") 
        self.log = logging.getLogger('thriftsvr.sock')
        self.cfg_addr = address
        sock = socket.socket(self.FAMILY, socket.SOCK_STREAM)
        bound = False

        logging.info("------------BaskSocket::init2") 
        self.sock = self.set_options(sock, bound=bound)
        logging.info("------------BaskSocket::init3") 

    def __str__(self):
        return "<socket %d>" % self.sock.fileno()

    def __getattr__(self, name):
        return getattr(self.sock, name)

    def set_options(self, sock, bound=False):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except socket.error as err:
            if err.errno not in (errno.ENOPROTOOPT, errno.EINVAL):
               raise
        if not bound:
            self.bind(sock)
        sock.setblocking(0)

        logging.info("------------BaskSocket::set_options") 
        # make sure that the socket can be inherited
        if hasattr(sock, "set_inheritable"):
            sock.set_inheritable(True)

        sock.listen(20)
        return sock

    def bind(self, sock):
        sock.bind(self.cfg_addr)

    def close(self):
        if self.sock is None:
            return

        try:
            self.sock.close()
        except socket.error as e:
            self.log.info("Error while closing socket %s", str(e))

        self.sock = None


class TCPSocket(BaseSocket):

    FAMILY = socket.AF_INET

    def __str__(self):
        scheme = "http"

        addr = self.sock.getsockname()
        return "%s://%s:%d" % (scheme, addr[0], addr[1])

    def set_options(self, sock, bound=False):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return super(TCPSocket, self).set_options(sock, bound=bound)


class TCP6Socket(TCPSocket):

    FAMILY = socket.AF_INET6

    def __str__(self):
        (host, port, _, _) = self.sock.getsockname()
        return "http://[%s]:%d" % (host, port)


def _sock_type(addr):
    if isinstance(addr, tuple):
        if util.is_ipv6(addr[0]):
            sock_type = TCP6Socket
        else:
            sock_type = TCPSocket
    else:
        raise TypeError("Unable to create socket from: %r" % addr)
    return sock_type


def create_sockets(laddr):
    """
    Create a new socket for the configured addresses or file descriptors.

    If a configured address is a tuple then a TCP socket is created.
    Otherwise, a TypeError is
    raised.
    """
    listeners = []

    for addr in laddr:
        sock_type = _sock_type(addr)
        logging.info("--------sock_type %s" % (sock_type.__name__))
        sock = None
        for i in range(5):
            try:
                sock = sock_type(addr)
                logging.info("--------sock")
            except socket.error as e:
                log = logging.getLogger('thriftsvr.sock')
                if e.args[0] == errno.EADDRINUSE:
                    log.error("Connection in use: %s", str(addr))
                if e.args[0] == errno.EADDRNOTAVAIL:
                    log.error("Invalid address: %s", str(addr))
                if i < 5:
                    msg = "connection to {addr} failed: {error}"
                    log.debug(msg.format(addr=str(addr), error=str(e)))
                    log.error("Retrying in 1 second.")
                    time.sleep(1)
            else:
                break

        if sock is None:
            log = logging.getLogger('thriftsvr.sock')
            log.error("Can't connect to %s", str(addr))
            sys.exit(1)

        listeners.append(sock)

    return listeners


def close_sockets(listeners, unlink=True):
    for sock in listeners:
        sock_name = sock.getsockname()
        sock.close()
