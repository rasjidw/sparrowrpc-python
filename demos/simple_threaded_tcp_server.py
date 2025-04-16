#!/usr/bin/env python3

import argparse
import logging
import sys
from time import sleep
from threading import current_thread

from lucido.core import make_export_decorator
from lucido.threaded import MsgChannelInjector, CallbackProxy
from lucido.engines import v050, _jsonrpc2l
from lucido.serialisers import MsgpackSerialiser, JsonSerialiser
from lucido.exceptions import InvalidParams

from lucido.threaded import ThreadPoolDispatcher, ThreadedMsgChannel
from lucido.threaded.transports import TcpListener
from lucido.threaded.transports.websockets import WebsocketListener


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
    jsonrpc2l_engine = _jsonrpc2l.ProtocolEngine()
    engine_choicies = [msgpack_engine, json_engine, jsonrpc2l_engine]
    
    dispatcher = ThreadPoolDispatcher(num_threads=5)
    if args.websocket:
        print('Running websocket server on 6000')
        websocket_server = WebsocketListener(engine_choicies, dispatcher)
        websocket_server.run_server('0.0.0.0', 6000)
    else:
        print('Running tcp server on 5000')
        tcp_server = TcpListener(engine_choicies, dispatcher)
        tcp_server.run_server('0.0.0.0', 5000)
    dispatcher.shutdown()


if __name__ == '__main__':
    main()
