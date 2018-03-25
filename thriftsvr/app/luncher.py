# -*- coding: utf-8 -*-

import sys
import argparse
from thriftsvr.arbiter import Arbiter
from thriftsvr import util


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('app_module', help='the app module name.')
    parser.add_argument('-b', '--bind', help='the ip:port to bind for listener. default:127.0.0.1:9999',
                        default="127.0.0.1:9999")
    parser.add_argument('-z', '--zkaddr', help="the zookeeper addr for exposing service. default:127.0.0.1:2181",
                        default="127.0.0.1:2181")
    parser.add_argument('-s', '--service', help="the service name exposed on zk.")
    parser.add_argument('-l', '--label', help="the labels expose with service")
    parser.add_argument('-w', '--workers', type=int, help='the number of workers.')
    parser.add_argument('-c', '--connections', type=int, help='the number of connections can handle per worker at same time.')
    parser.add_argument('-n', '--proc_name', help='the name of the process.')
    parser.add_argument('-v', '--verbose', help='the name of the process.',
                        action='store_true')
    parser.add_argument('-d', '--daemonize', help='make service become a daemon.',
                        action="store_true")

    args = parser.parse_args()

    args.bind = util.parse_address(args.bind)
    return args

def run():
    """\
    The ``thriftsvr`` command line runner.
    """
    try:
        conf = parse_args()
        print("conf:", conf)
        if conf.verbose:
            import logging
            logging.basicConfig(level=logging.DEBUG)
        Arbiter(conf).run()
    except RuntimeError as e:
        print("\nError: %s\n" % e, file=sys.stderr)
        sys.stderr.flush()
        sys.exit(1)

if __name__ == "__main__":
    run()
