#!/usr/bin/env python3

import argparse
import logging
import sys
from time import sleep
from threading import current_thread

from sparrowrpc.core import make_export_decorator
from sparrowrpc.threaded import ThreadedMsgChannelInjector, ThreadedCallbackProxy
from sparrowrpc.engines import jr2l, v050
from sparrowrpc.serialisers import MsgpackSerialiser, JsonSerialiser
from sparrowrpc.exceptions import InvalidParams

from sparrowrpc.threaded import ThreadedDispatcher, ThreadedMsgChannel
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

    json_engine = v050.ProtocolEngine(JsonSerialiser())
    msgpack_engine = v050.ProtocolEngine(MsgpackSerialiser())
    jsonrpc2l_engine = jr2l.ProtocolEngine()
    engine_choicies = [msgpack_engine, json_engine, jsonrpc2l_engine]
    
    dispatcher = ThreadedDispatcher(num_threads=5)
    if args.websocket:
        print('Running websocket server on 6000')
        websocket_server = ThreadedWebsocketListener(engine_choicies, dispatcher)
        websocket_server.run_server('0.0.0.0', 6000)
    else:
        print('Running tcp server on 5000')
        tcp_server = ThreadedTcpListener(engine_choicies, dispatcher)
        tcp_server.run_server('0.0.0.0', 5000)
    dispatcher.shutdown()


if __name__ == '__main__':
    main()
