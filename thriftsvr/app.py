# -*- coding: utf-8 -
#

import os
import sys
import traceback

from thriftsvr import util
from thriftsvr.arbiter import Arbiter

class BaseApplication(object):
    def run(self):
        try:
            Arbiter(self).run()
        except RuntimeError as e:
            print("\nError: %s\n" % e, file=sys.stderr)
            sys.stderr.flush()
            sys.exit(1)


class Application(BaseApplication):
    def run(self):
        #if self.daemonize:
        if False:
            util.daemonize(self.cfg.enable_stdio_inheritance)

        super(Application, self).run()

class ThriftApplication(Application):
    '''
       初始化应用。
       addr  监听地址('127.0.0.1', 9090)
       tfactory thrift transport factory
       pfactory thrift protocol factory
    '''
    def __init__(self, processor, addr, tfactory, pfactory):
        self.processor = processor
        self.address = addr
        self.tfactory = tfactory
        self.pfactory = pfactory 
        self.workers = 3
        self.worker_connections = 10
        self.proc_name = "test"


