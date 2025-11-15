#!/usr/bin/env python3

import argparse
import logging
import sys

from sparrowrpc.decorators import make_export_decorator
from sparrowrpc.exceptions import InvalidParams
from sparrowrpc.engine import ProtocolEngine

from sparrowrpc.threaded import ThreadedDispatcher
from sparrowrpc.threaded.transports import ThreadedTcpListener
from sparrowrpc.threaded.transports.websockets import ThreadedWebsocketListener


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
                    )


export = make_export_decorator()


@export
def hello_world(name=None):
    if name:
        return f'Hello {name}!'
    else:
        return f'Hello world!'



@export
def division(a, b):
    try:
        result = a / b
    except (TypeError, ValueError) as e:
        raise InvalidParams(f'Invalid param type: {str(e)}')
    except ZeroDivisionError:
        raise InvalidParams('b must not be 0')
    if a == 11:
        raise RuntimeError('a == 11 is a bug')
    return result



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--websocket', action='store_true')
    args = parser.parse_args()

    engine = ProtocolEngine()

    dispatcher = ThreadedDispatcher(num_threads=5)
    if args.websocket:
        print('Running websocket server on 9001')
        websocket_server = ThreadedWebsocketListener(engine, dispatcher)
        websocket_server.run_server('0.0.0.0', 9001)
    else:
        print('Running tcp server on 5000')
        tcp_server = ThreadedTcpListener(engine, dispatcher)
        tcp_server.run_server('0.0.0.0', 5000)
    dispatcher.shutdown()


if __name__ == '__main__':
    main()
