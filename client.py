#!/usr/bin/env python

from gevent import monkey; monkey.patch_all()

import sys
sys.path.append('./gen-py')

from watson import logging
from watson.ttypes import LogException

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from thriftpool import client
import traceback
import time

c = client.Client(iface_cls=logging.Client, host='localhost', port=9090, pool_size=3, retries=300)

while True:
    time.sleep(1)
    try:
        c.log('hello thrift')
    except:
        traceback.print_exc()


