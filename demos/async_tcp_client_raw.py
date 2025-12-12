#!/usr/bin/env python3

import argparse
import asyncio
import logging
import sys


from sparrowrpc.decorators import make_export_decorator
from sparrowrpc.messages import ExceptionResponse, Response, Request, RequestCallbackInfo
from sparrowrpc.engine import ProtocolEngine

from sparrowrpc.asyncio import AsyncDispatcher
from sparrowrpc.asyncio.transports import AsyncTcpConnector


if sys.implementation.name == 'micropython':
    print('**** MICROPYTHON ****')
    AsyncWebsocketConnector = None
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
else:
    from sparrowrpc.asyncio.transports.websockets import AsyncWebsocketConnector
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')


def get_thread_or_task_name():
    name_getter = getattr(asyncio.current_task(), 'get_name', None)
    if name_getter:
        return name_getter()
    else:
        return 'dummy'


export = make_export_decorator()

@export
async def display_chat_message(msg):
    print('********** Got incoming message **********')
    print(msg)                    
    print('**************************************')
    print()


async def show_progress(message):
    print('--- Progress Msg ---')
    print(message)
    print('--------------------')


async def show_data(data):
    print(f'Got data: {data}')


class ResultWaiter:
    def __init__(self):
        self.got_result = asyncio.Event()
        self.messages = []
        self.result = None
        self.exception = None

    async def process_msg(self, msg):
        await show_data(msg)
        if isinstance(msg, Response):
            self.result = msg.result
            self.got_result.set()
        elif isinstance(msg, ExceptionResponse):
            self.exception = msg.exc_info
            self.got_result.set()
        else:
            self.messages.append(msg)

    async def get_result(self):
        await self.got_result.wait()
        if self.exception:
            raise RuntimeError('Something bad happened')
        else:
            return self.result


async def background_counter(channel, count_to, delay):
    cb = lambda message: print(f'*** Got msg: {message}. Displaying in thread {get_thread_or_task_name()}')
    proxy = await channel.get_proxy()
    print(f'*** Calling slow counter in background thread {get_thread_or_task_name()}')
    req = Request('slow_counter', params=dict(count_to=count_to, delay=delay), callback_params={'progress': RequestCallbackInfo(cb)})
    result = await proxy.send_request(req)
    print(f'*** Background counter result: {result}')


async def test_calls(use_msgpack, use_websocket):
    if use_msgpack:
        default_serialiser_sig = 'm'
    else:
        default_serialiser_sig = 'j'
    engine = ProtocolEngine()
    dispatcher = AsyncDispatcher(num_threads=5)
    if use_websocket:
        connector = AsyncWebsocketConnector(engine, dispatcher, default_serialiser_sig=default_serialiser_sig)
        uri = f'ws://127.0.0.1:9001/'
        channel = await connector.connect(uri)
    else:
        connector = AsyncTcpConnector(engine, dispatcher, default_serialiser_sig=default_serialiser_sig)
        channel = await connector.connect('127.0.0.1', 5000)
    await channel.start_channel()
    
    # start of test calls
    proxy = channel.get_proxy()

    req = Request(target='ping', namespace='#sys')
    result = await proxy.send_request(req)
    print(f'Got result: {result}')

    req = Request(target='hello_world')
    req.acknowledge = True
    result = await proxy.send_request(req, msg_sent_callback=show_data, ack_callback=show_data)
    print(f'Got result: {result}')

    name = 'Fred'
    req = Request(target='hello_world', params={'name': name})
    result = await proxy.send_request(req)
    print(f'Got result: {result}')

    req = Request(target='slow_counter', params={'count_to': 5}, callback_params={'progress': RequestCallbackInfo(func=show_progress)})
    result = await proxy.send_request(req)
    print(f'Got result: {result}')

    await channel.shutdown_channel()
    print('Channel shut down')
    await dispatcher.shutdown()

    print('Done')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--msgpack', action='store_true')
    if AsyncWebsocketConnector:
        parser.add_argument('--websocket', action='store_true')
    args = parser.parse_args()

    root_logger = logging.getLogger()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)  # does not seem to have an effect in MicroPython

    asyncio.run(test_calls(args.msgpack, args.websocket))


if __name__ == '__main__':
    main()