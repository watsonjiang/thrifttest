# -*- coding: utf-8 -
#
from gevent import monkey
monkey.patch_all()

import errno
import os
import random
import select
import signal
import sys
import time
import traceback
import logging
import json

from kazoo.client import KazooClient
from kazoo.client import KazooState

from thriftsvr.errors import HaltServer
from thriftsvr import sock, util

from thriftsvr import SERVER_SOFTWARE
from thriftsvr.worker import GeventWorker

class Arbiter(object):
    """
    Arbiter maintain the workers processes alive. It launches or
    kills them if needed. It also manages application reloading
    via SIGHUP/USR2.
    """

    # A flag indicating if a worker failed to
    # to boot. If a worker process exist with
    # this error code, the arbiter will terminate.
    WORKER_BOOT_ERROR = 3

    # A flag indicating if an application failed to be loaded
    APP_LOAD_ERROR = 4

    LISTENERS = []
    WORKERS = {}
    PIPE = []

    # I love dynamic languages
    SIG_QUEUE = []
    SIGNALS = [getattr(signal, "SIG%s" % x)
               for x in "HUP QUIT INT TERM TTIN TTOU USR1 USR2 WINCH".split()]
    SIG_NAMES = dict(
        (getattr(signal, name), name[3:].lower()) for name in dir(signal)
        if name[:3] == "SIG" and name[3] != "_"
    )

    def __init__(self, conf):
        self._num_workers = conf.workers
        self._last_logged_active_worker_count = None
        self.log = logging.getLogger('thriftsvr.Arbiter')
        
        self.conf = conf
        self.address = conf.bind
        self.num_workers = self.conf.workers
        self.proc_name = self.conf.proc_name
        self.pidfile = None
        self.worker_age = 0
        self.reexec_pid = 0
        self.master_pid = 0
        self.master_name = "Master"
        self.graceful_timeout = 3
        self.timeout = 3

    def start(self):
        """
        Initialize the arbiter. Start listening and set pidfile if needed.
        """
        self.log.info("Starting %s", SERVER_SOFTWARE)

        self.pid = os.getpid()
        
        self.init_signals()
        #创建socket
        self.LISTENERS = sock.create_sockets([self.address])

        listeners_str = ",".join([str(l) for l in self.LISTENERS])
        self.log.debug("Master booted")
        self.log.info("Listening at: %s (%s)", listeners_str, self.pid)

    def init_zk(self):
        #没有service name就不去zk注册
        if not self.conf.service:
            self.log.warning('Service is empty. will not expose service to zk.')
            return

        from kazoo.handlers.gevent import SequentialGeventHandler
        self.is_service_exposed = False
        self.zk = KazooClient(hosts='localhost:2181', handler=SequentialGeventHandler(),
                              connection_retry={'max_tries':-1, 'max_delay':10})
        def _listener(state):
            logging.info("state:{}".format(state))
            if state == KazooState.LOST:
                self.is_service_exposed = False
            elif state == KazooState.CONNECTED:
                if not self.is_service_exposed:
                    import threading
                    threading.Thread(target=self.expose_on_zk).start()
        self.zk.add_listener(_listener)
        self.zk.start()

    def expose_on_zk(self):
        e = {}
        conf = self.conf
        e['service'] = conf.service
        e['label'] = {}
        e['bind'] = conf.bind
        value = json.dumps(e).encode()
        service_path = '/thriftsvr/{}'.format(conf.service.replace('.', '/'))
        self.zk.ensure_path(service_path)
        node_path = '{}/node-'.format(service_path)
        self.zk.create(node_path.format(conf.service), value, ephemeral=True, sequence=True, makepath=True)

    def init_signals(self):
        """
        Initialize master signal handling. Most of the signals
        are queued. Child signals only wake up the master.
        """
        # close old PIPE
        for p in self.PIPE:
            os.close(p)

        # initialize the pipe
        self.PIPE = pair = os.pipe()
        for p in pair:
            util.set_non_blocking(p)
            util.close_on_exec(p)

        # initialize all signals
        for s in self.SIGNALS:
            signal.signal(s, self.signal)
        signal.signal(signal.SIGCHLD, self.handle_chld)

    def signal(self, sig, frame):
        if len(self.SIG_QUEUE) < 5:
            self.SIG_QUEUE.append(sig)
            self.wakeup()

    def run(self):
        """Main master loop."""
        self.start()
        util._setproctitle("master [%s]" % self.proc_name)

        try:
            self.manage_workers()
            #启动worker后再注册到zk，避免注册过早造成请求失败
            self.init_zk()
            while True:
                sig = self.SIG_QUEUE.pop(0) if self.SIG_QUEUE else None
                if sig is None:
                    self.sleep()
                    self.murder_workers()
                    self.manage_workers()
                    continue

                if sig not in self.SIG_NAMES:
                    self.log.info("Ignoring unknown signal: %s", sig)
                    continue

                signame = self.SIG_NAMES.get(sig)
                handler = getattr(self, "handle_%s" % signame, None)
                if not handler:
                    self.log.error("Unhandled signal: %s", signame)
                    continue
                self.log.info("Handling signal: %s", signame)
                handler()
                self.wakeup()
        except StopIteration:
            self.halt()
        except KeyboardInterrupt:
            self.halt()
        except HaltServer as inst:
            self.halt(reason=inst.reason, exit_status=inst.exit_status)
        except SystemExit:
            raise
        except Exception:
            self.log.info("Unhandled exception in main loop",
                          exc_info=True)
            self.stop(False)
            if self.pidfile is not None:
                self.pidfile.unlink()
            sys.exit(-1)

    def handle_chld(self, sig, frame):
        "SIGCHLD handling"
        self.reap_workers()
        self.wakeup()

    def handle_hup(self):
        """\
        HUP handling.
        - Reload configuration
        - Start the new worker processes with a new configuration
        - Gracefully shutdown the old worker processes
        """
        self.log.info("Hang up: %s", self.master_name)
        self.reload()

    def handle_term(self):
        "SIGTERM handling"
        raise StopIteration

    def handle_int(self):
        "SIGINT handling"
        self.stop(False)
        raise StopIteration

    def handle_quit(self):
        "SIGQUIT handling"
        self.stop(False)
        raise StopIteration

    def handle_ttin(self):
        """\
        SIGTTIN handling.
        Increases the number of workers by one.
        """
        self.num_workers += 1
        self.manage_workers()

    def handle_ttou(self):
        """\
        SIGTTOU handling.
        Decreases the number of workers by one.
        """
        if self.num_workers <= 1:
            return
        self.num_workers -= 1
        self.manage_workers()

    def handle_usr1(self):
        """\
        SIGUSR1 handling.
        Kill all workers by sending them a SIGUSR1
        """
        self.log.reopen_files()
        self.kill_workers(signal.SIGUSR1)

    def handle_winch(self):
        """SIGWINCH handling"""
        if self.cfg.daemon:
            self.log.info("graceful stop of workers")
            self.num_workers = 0
            self.kill_workers(signal.SIGTERM)
        else:
            self.log.debug("SIGWINCH ignored. Not daemonized")

    def wakeup(self):
        """\
        Wake up the arbiter by writing to the PIPE
        """
        try:
            os.write(self.PIPE[1], b'.')
        except IOError as e:
            if e.errno not in [errno.EAGAIN, errno.EINTR]:
                raise

    def halt(self, reason=None, exit_status=0):
        """ halt arbiter """
        self.stop()
        self.log.info("Shutting down: %s", self.master_name)
        if reason is not None:
            self.log.info("Reason: %s", reason)
        if self.pidfile is not None:
            self.pidfile.unlink()
        sys.exit(exit_status)

    def sleep(self):
        """\
        Sleep until PIPE is readable or we timeout.
        A readable PIPE means a signal occurred.
        """
        try:
            ready = select.select([self.PIPE[0]], [], [], 1.0)
            if not ready[0]:
                return
            while os.read(self.PIPE[0], 1):
                pass
        except (select.error, OSError) as e:
            # TODO: select.error is a subclass of OSError since Python 3.3.
            error_number = getattr(e, 'errno', e.args[0])
            if error_number not in [errno.EAGAIN, errno.EINTR]:
                raise
        except KeyboardInterrupt:
            sys.exit()

    def stop(self, graceful=True):
        """\
        Stop workers

        :attr graceful: boolean, If True (the default) workers will be
        killed gracefully  (ie. trying to wait for the current connection)
        """

        unlink = False
        sock.close_sockets(self.LISTENERS, unlink)

        self.LISTENERS = []
        sig = signal.SIGTERM
        if not graceful:
            sig = signal.SIGQUIT
        limit = time.time() + self.graceful_timeout
        # instruct the workers to exit
        self.kill_workers(sig)
        # wait until the graceful timeout
        while self.WORKERS and time.time() < limit:
            time.sleep(0.1)

        self.kill_workers(signal.SIGKILL)

    def murder_workers(self):
        """\
        Kill unused/idle workers
        """
        if not self.timeout:
            return
        workers = list(self.WORKERS.items())
        for (pid, worker) in workers:
            try:
                if time.time() - worker.tmp.last_update() <= self.timeout:
                    continue
            except (OSError, ValueError):
                continue

            if not worker.aborted:
                self.log.critical("WORKER TIMEOUT (pid:%s)", pid)
                worker.aborted = True
                self.kill_worker(pid, signal.SIGABRT)
            else:
                self.kill_worker(pid, signal.SIGKILL)

    def reap_workers(self):
        """\
        Reap workers to avoid zombie processes
        """
        try:
            while True:
                wpid, status = os.waitpid(-1, os.WNOHANG)
                if not wpid:
                    break
                # A worker was terminated. If the termination reason was
                # that it could not boot, we'll shut it down to avoid
                # infinite start/stop cycles.
                exitcode = status >> 8
                if exitcode == self.WORKER_BOOT_ERROR:
                    reason = "Worker failed to boot."
                    raise HaltServer(reason, self.WORKER_BOOT_ERROR)
                if exitcode == self.APP_LOAD_ERROR:
                    reason = "App failed to load."
                    raise HaltServer(reason, self.APP_LOAD_ERROR)

                worker = self.WORKERS.pop(wpid, None)
                if not worker:
                    continue
                worker.tmp.close()
        except OSError as e:
            if e.errno != errno.ECHILD:
                raise

    def manage_workers(self):
        """\
        Maintain the number of workers by spawning or killing
        as required.
        """
        if len(self.WORKERS.keys()) < self.num_workers:
            self.spawn_workers()

        workers = self.WORKERS.items()
        workers = sorted(workers, key=lambda w: w[1].age)
        while len(workers) > self.num_workers:
            (pid, _) = workers.pop(0)
            self.kill_worker(pid, signal.SIGTERM)

        active_worker_count = len(workers)
        if self._last_logged_active_worker_count != active_worker_count:
            self._last_logged_active_worker_count = active_worker_count
            self.log.debug("{0} workers".format(active_worker_count),
                           extra={"metric": "gunicorn.workers",
                                  "value": active_worker_count,
                                  "mtype": "gauge"})

    def spawn_worker(self):
        self.worker_age += 1
        worker = GeventWorker(self.worker_age, self.pid, self.LISTENERS,
                              self.conf.app_module, self.conf.connections,
                              self.timeout / 2.0)
        pid = os.fork()
        if pid != 0:
            worker.pid = pid
            self.WORKERS[pid] = worker
            return pid

        #Do not inherit the temporary files of other workers
        for sibling in self.WORKERS.values():
            sibling.tmp.close()

        # Process Child
        worker.pid = os.getpid()
        try:
            util._setproctitle("worker [%s]" % self.proc_name)
            self.log.info("Booting worker with pid: %s", worker.pid)
            worker.init_process()
            sys.exit(0)
        except SystemExit:
            raise
        except:
            self.log.exception("Exception in worker process")
            if not worker.booted:
                sys.exit(self.WORKER_BOOT_ERROR)
            sys.exit(-1)
        finally:
            self.log.info("Worker exiting (pid: %s)", worker.pid)
            try:
                worker.tmp.close()
            except:
                self.log.warning("Exception during worker exit:\n%s",
                                  traceback.format_exc()) 

    def spawn_workers(self):
        """\
        Spawn new workers as needed.

        This is where a worker process leaves the main loop
        of the master process.
        """

        for _ in range(self.num_workers - len(self.WORKERS.keys())):
            self.spawn_worker()
            time.sleep(0.1 * random.random())

    def kill_workers(self, sig):
        """\
        Kill all workers with the signal `sig`
        :attr sig: `signal.SIG*` value
        """
        worker_pids = list(self.WORKERS.keys())
        for pid in worker_pids:
            self.kill_worker(pid, sig)

    def kill_worker(self, pid, sig):
        """\
        Kill a worker

        :attr pid: int, worker pid
        :attr sig: `signal.SIG*` value
         """
        try:
            os.kill(pid, sig)
        except OSError as e:
            if e.errno == errno.ESRCH:
                try:
                    worker = self.WORKERS.pop(pid)
                    worker.tmp.close()
                    return
                except (KeyError, OSError):
                    return
            raise
