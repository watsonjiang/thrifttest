from gevent import monkey; monkey.patch_all()
import sys
import logging
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.DEBUG)

# your gen-py dir
sys.path.append('gen-py')

# Example files
from watson import logging
from watson.ttypes import LogException

# Thrift files
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thriftsvr.app import ThriftApplication

import os

class LoggingHandler:
    # return current time stamp
    def log(self, message):
       print("pid:{} {}".format(os.getpid(), message))

    def log1(self, message):
       print("hello", message)
       raise LogException(code=1, reason='fuck') 

# set handler to our implementation
handler = LoggingHandler()

processor = logging.Processor(handler)
tfactory = TTransport.TFramedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()

app = ThriftApplication(processor, tfactory, pfactory)

if __name__ == '__main__':
   print('Starting server')
   app.run()
