import logging
from kazoo.client import KazooClient
from kazoo.client import DataWatch
from kazoo.client import ChildrenWatch

logging.basicConfig(level=logging.DEBUG)

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

endpoint = []

def on_peer_added(peers):
    logging.info("---------------add peer:{}".format(peers))

def on_peer_removed(peers):
    logging.info("---------------rm peer:{}".format(peers))


@zk.ChildrenWatch('/watson')
def foo(children):
    global endpoint
    newset = set(children)
    oldset = set(endpoint)
    endpoint = children
    added = newset - oldset
    if added:
        on_peer_added(added)
    removed = oldset - newset
    if removed:
        on_peer_removed(removed)
    

while True:
    import time
    time.sleep(1)
