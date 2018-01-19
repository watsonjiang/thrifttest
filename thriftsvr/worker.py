# -*- coding: utf-8 -
#
from datetime import datetime
import os
from random import randint
import signal
import sys
import time
import traceback
import logging
from functools import partial

_socket = __import__("socket")
import gevent
from gevent.pool import Pool
from gevent.server import StreamServer
from gevent.socket import wait_write, socket

from thriftsvr import util


class Worker(object):

    SIGNALS = [getattr(signal, "SIG%s" % x)
            for x in "ABRT HUP QUIT INT TERM USR1 USR2 WINCH CHLD".split()]

    PIPE = []

    def __init__(self, age, ppid, sockets, app, timeout):
        """\
        This is called pre-fork so it shouldn't do anything to the
        current process. If there's a need to make process wide
        changes you'll want to do that in ``self.init_process()``.
        """
        self.age = age
        self.pid = "[booting]"
        self.ppid = ppid
        self.sockets = sockets
        self.app = app
        self.timeout = timeout
        self.booted = False
        self.aborted = False
        self.graceful_timeout = 3  #服务器停止后等3秒优雅结束
        self.worker_connections = app.worker_connections
        self.nr = 0
        self.alive = True
        self.log = logging.getLogger('thriftsvr.worker')

    def __str__(self):
        return "<Worker %s>" % self.pid

    def notify(self):
        """\
        Your worker subclass must arrange to have this method called
        once every ``self.timeout`` seconds. If you fail in accomplishing
        this task, the master process will murder your workers.
        """
        #self.tmp.notify()

    def run(self):
        """\
        This is the mainloop of a worker process. You should override
        this method in a subclass to provide the intended behaviour
        for your particular evil schemes.
        """
        raise NotImplementedError()

    def init_process(self):
        """\
        If you override this method in a subclass, the last statement
        in the function should be to call this method with
        super(MyWorkerClass, self).init_process() so that the ``run()``
        loop is initiated.
        """

        # Reseed the random number generator
        util.seed()

        # For waking ourselves up
        self.PIPE = os.pipe()
        for p in self.PIPE:
            util.set_non_blocking(p)
            util.close_on_exec(p)

        # Prevent fd inheritance
        for s in self.sockets:
            util.close_on_exec(s)

        self.wait_fds = self.sockets + [self.PIPE[0]]

        self.init_signals()

        # Enter main run loop
        self.booted = True
        self.run()

    def init_signals(self):
        # reset signaling
        for s in self.SIGNALS:
            signal.signal(s, signal.SIG_DFL)
        # init new signaling
        signal.signal(signal.SIGQUIT, self.handle_quit)
        signal.signal(signal.SIGTERM, self.handle_exit)
        signal.signal(signal.SIGINT, self.handle_quit)
        signal.signal(signal.SIGWINCH, self.handle_winch)
        signal.signal(signal.SIGUSR1, self.handle_usr1)
        signal.signal(signal.SIGABRT, self.handle_abort)

        # Don't let SIGTERM and SIGUSR1 disturb active requests
        # by interrupting system calls
        if hasattr(signal, 'siginterrupt'):  # python >= 2.6
            signal.siginterrupt(signal.SIGTERM, False)
            signal.siginterrupt(signal.SIGUSR1, False)

        if hasattr(signal, 'set_wakeup_fd'):
            signal.set_wakeup_fd(self.PIPE[1])

    def handle_usr1(self, sig, frame):
        self.log.reopen_files()

    def handle_exit(self, sig, frame):
        self.alive = False

    def handle_quit(self, sig, frame):
        self.alive = False
        # worker_int callback
        time.sleep(0.1)
        sys.exit(0)

    def handle_abort(self, sig, frame):
        self.alive = False
        sys.exit(1)

    def handle_winch(self, sig, fname):
        # Ignore SIGWINCH in worker. Fixes a crash on OpenBSD.
        self.log.debug("worker: SIGWINCH ignored.")


class GeventWorker(Worker):

    def patch(self):
        from gevent import monkey
        monkey.noisy = False

        # if the new version is used make sure to patch subprocess
        if gevent.version_info[0] == 0:
            monkey.patch_all()
        else:
            monkey.patch_all(subprocess=True)


        # patch sockets
        sockets = []
        for s in self.sockets:
            if sys.version_info[0] == 3:
                sockets.append(socket(s.FAMILY, _socket.SOCK_STREAM,
                    fileno=s.sock.fileno()))
            else:
                sockets.append(socket(s.FAMILY, _socket.SOCK_STREAM,
                    _sock=s))
        self.sockets = sockets

    def notify(self):
        super(GeventWorker, self).notify()
        if self.ppid != os.getppid():
            self.log.info("Parent changed, shutting down: %s", self)
            sys.exit(0)

    def handle(self, listener, client, addr):
        #client转换成Thrift识别的对象
        tclient = TSocket()
        tclient.setHandle(client)
        itrans = self.inputTransportFactory.getTransport(client)
        otrans = self.outputTransportFactory.getTransport(client)
        iprot = self.inputProtocolFactory.getProtocol(itrans)
        oprot = self.outputProtocolFactory.getProtocol(otrans)
        try:
            processor = self.app.processor
            while True:
                processor.process(iprot, oprot)
        except TTransport.TTransportException:
            pass
        except Exception as x:
            pass

        itrnas.close()
        otrans.close()

    def run(self):
        servers = []
        ssl_args = {}

        for s in self.sockets:
            s.setblocking(1)
            pool = Pool(self.worker_connections)
            hfun = partial(self.handle, s)
            server = StreamServer(s, handle=hfun, spawn=pool, **ssl_args)

            server.start()
            servers.append(server)

        while self.alive:
            self.notify()
            gevent.sleep(1.0)

        try:
            # Stop accepting requests
            for server in servers:
                server.close()

            # Handle current requests until graceful_timeout
            ts = time.time()
            while time.time() - ts <= self.graceful_timeout:
                accepting = 0
                for server in servers:
                    if server.pool.free_count() != server.pool.size:
                        accepting += 1

                # if no server is accepting a connection, we can exit
                if not accepting:
                    return

                self.notify()
                gevent.sleep(1.0)

            # Force kill all active the handlers
            self.log.warning("Worker graceful timeout (pid:%s)" % self.pid)
            for server in servers:
                server.stop(timeout=1)
        except:
            pass

    def handle(self, listener, client, addr):
        # Connected socket timeout defaults to socket.getdefaulttimeout().
        # This forces to blocking mode.
        client.setblocking(1)
        super(GeventWorker, self).handle(listener, client, addr)

    def handle_quit(self, sig, frame):
        # Move this out of the signal handler so we can use
        # blocking calls. See #1126
        gevent.spawn(super(GeventWorker, self).handle_quit, sig, frame)


    def init_process(self):
        # monkey patch here
        self.patch()

        # reinit the hub
        from gevent import hub
        hub.reinit()

        # then initialize the process
        super(GeventWorker, self).init_process()


