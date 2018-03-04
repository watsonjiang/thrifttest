import logging
from kazoo.client import KazooClient
import json
from kazoo.client import KazooState
import threading

logging.basicConfig(level=logging.DEBUG)

zk = KazooClient(hosts='192.168.1.103:2181')

print(type(zk.handler))

is_registed = False

@zk.add_listener
def listener(state):
    global is_registed
    logging.info("state:{}".format(state))
    if state == KazooState.LOST:
        is_registed = False
    elif state == KazooState.CONNECTED:
        if not is_registed:
            threading.Thread(target=register).start()


def register():
   e = {}
   e['service'] = 'com.watson.orion'
   e['version'] = '0.0.1'
   e['label'] = ''
   e['ip'] = '127.0.0.1'
   e['port'] = 12345
   e['extra'] = {}
   value = json.dumps(e).encode()
   zk.create('/watson/a', value, ephemeral=True, sequence=True, makepath=True)


zk.start()

while True:
    import time
    time.sleep(1)
