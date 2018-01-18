from gevent import monkey; monkey.patch_all()
import sys
# your gen-py dir
sys.path.append('gen-py')

# Example files
from watson import logging
from watson.ttypes import LogException

# Thrift files
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

# Server implementation
class LoggingHandler:
    # return current time stamp
    def log(self, message):
       print(message)
       raise LogException(code=2, reason='thank')

    def log1(self, message):
       print("hello", message)
       raise LogException(code=1, reason='fuck') 

# set handler to our implementation
handler = LoggingHandler()

processor = logging.Processor(handler)
transport = TSocket.TServerSocket(port=9090)
tfactory = TTransport.TFramedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()

# set server
server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

print('Starting server')
server.serve()
