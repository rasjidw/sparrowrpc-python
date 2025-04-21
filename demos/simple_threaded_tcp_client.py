#!/usr/bin/env python3

import argparse
import logging
import sys
import threading
import time

from lucido.core import make_export_decorator, IncomingResponse, IncomingException
from lucido.serialisers import MsgpackSerialiser, JsonSerialiser
from lucido.engines import v050, _jsonrpc2l

from lucido.threaded import ThreadedDispatcher
from lucido.threaded.transports import TcpConnector
from lucido.threaded.transports.websockets import WebsocketConnector


logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')


export = make_export_decorator()

@export
def display_message(msg):
    print('********** Got incoming message **********')
    print(msg)                    
    print('**************************************')
    print()



def show_data(data):
    print(f'Got data: {data}')


class ResultWaiter:
    def __init__(self):
        self.got_result = threading.Event()
        self.messages = []
        self.result = None
        self.exception = None

    def process_msg(self, msg):
        show_data(msg)
        if isinstance(msg, IncomingResponse):
            self.result = msg.result
            self.got_result.set()
        elif isinstance(msg, IncomingException):
            self.exception = msg.exc_info
            self.got_result.set()
        else:
            self.messages.append(msg)

    def get_result(self):
        self.got_result.wait()
        if self.exception:
            raise RuntimeError('Something bad happended')
        else:
            return self.result


def background_counter(channel, count_to, delay):
    print(f'*** Calling slow counter in background thread {threading.current_thread().name}')

    cb = lambda message: print(f'*** Got msg: {message}. Displaying in thread {threading.current_thread().name}')
    result = channel.request.slow_counter(count_to=count_to, delay=delay, progress=cb)

    print(f'*** Background counter result: {result}')


def main(engine_choice, use_websocket):
    if engine_choice == 'jsonrpc2':
        engine = _jsonrpc2l.ProtocolEngine()
    else:
        if engine_choice == 'mp':
            serialiser = MsgpackSerialiser()
        else:
            serialiser = JsonSerialiser()
        engine = v050.ProtocolEngine(serialiser)
        
    dispatcher = ThreadedDispatcher(num_threads=5)
    if use_websocket:
        connector = WebsocketConnector(engine, dispatcher)
        engine_sig = engine.get_engine_signature()
        uri = f'ws://127.0.0.1:6000/{engine_sig}'
        channel = connector.connect(uri)
    else:
        connector = TcpConnector(engine, dispatcher)
        channel = connector.connect('127.0.0.1', 5000)
    channel.start_channel()

    # start of test calls

    result = channel.request(namespace='#sys').ping()
    print(f'Ping result: {result}')

    result = channel.request.hello_world()
    print(result)

    result = channel.request.hello_world(name='Bill')
    print(result)

    channel.shutdown_channel()
    dispatcher.shutdown()

    print('Done')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--msgpack', action='store_true')
    group.add_argument('--jsonrpc2', action='store_true')
    parser.add_argument('--websocket', action='store_true')
    args = parser.parse_args()

    root_logger = logging.getLogger()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
    choice = 'js'
    if args.msgpack:
        choice = 'mp'
    if args.jsonrpc2:
        choice = 'jsonrpc2'
    main(choice, args.websocket)
