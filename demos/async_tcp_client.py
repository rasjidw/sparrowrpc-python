#!/usr/bin/env python3

import argparse
import asyncio
import logging
import sys

from sparrowrpc import export
from sparrowrpc.messages import IncomingException, IncomingResponse
from sparrowrpc.serialisers import JsonSerialiser
try:
    from sparrowrpc.serialisers import MsgpackSerialiser
except ImportError:
     MsgpackSerialiser = None
try:
    from sparrowrpc.serialisers import CborSerialiser
except ImportError:
    CborSerialiser = None
from sparrowrpc.engines.v050 import ProtocolEngine

from sparrowrpc.asyncio import AsyncDispatcher
from sparrowrpc.asyncio.transports import AsyncTcpConnector, AsyncUnixSocketConnector
try:
    from sparrowrpc.asyncio.transports.websockets import AsyncWebsocketConnector
except ImportError:
    AsyncWebsocketConnector = None


if sys.implementation.name == 'micropython':
    print('**** MICROPYTHON ****')
    logging.basicConfig(level=logging.INFO)
    micropython = True
else:
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
    micropython = False


def get_thread_or_task_name():
    name_getter = getattr(asyncio.current_task(), 'get_name', None)
    if name_getter:
        return name_getter()
    else:
        return 'dummy'


@export
def display_chat_message(msg):
    print('********** Got incoming message **********')
    print(msg)                    
    print('**************************************')
    print()


def show_progress(message):
    print(f'Running callback in {get_thread_or_task_name()}')
    print(message)
    print('--------------')


def show_data(data):
    print(f'Got data: {data}')


class ResultWaiter:
    def __init__(self):
        self.got_result = asyncio.Event()
        self.messages = []
        self.result = None
        self.exception = None

    async def process_msg(self, msg):
        show_data(msg)
        if isinstance(msg, IncomingResponse):
            self.result = msg.result
            await self.got_result.set()
        elif isinstance(msg, IncomingException):
            self.exception = msg.exc_info
            await self.got_result.set()
        else:
            self.messages.append(msg)

    async def get_result(self):
        await self.got_result.wait()
        if self.exception:
            raise RuntimeError('Something bad happended')
        else:
            return self.result


async def background_counter(channel, count_to, delay):
    print(f'*** Calling slow counter in background task {get_thread_or_task_name()}')

    cb = lambda message: print(f'*** Got msg: {message}. Displaying in task {get_thread_or_task_name()}')
    result = await channel.request.slow_counter(count_to=count_to, delay=delay, progress=cb)

    print(f'*** Background counter result: {result}')


# For Micropython
class AsyncData:
    def __init__(self, iter_data):
        self.iter_data = iter_data
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            result = self.iter_data[self.index]
            self.index += 1
            return result
        except IndexError:
            raise StopAsyncIteration()


async def main(args):
    if args.msgpack:
        serialiser = MsgpackSerialiser()
    elif args.cbor:
        serialiser = CborSerialiser()
    else:
        serialiser = JsonSerialiser()
    engine = ProtocolEngine(serialiser)
    print(f'Engine signature is: {engine.get_engine_signature()}')

    dispatcher = AsyncDispatcher(num_threads=5)
    if getattr(args, 'websocket', None):
        connector = AsyncWebsocketConnector(engine, dispatcher)
        engine_sig = engine.get_engine_signature()
        uri = f'ws://127.0.0.1:9001/{engine_sig}'
        channel = await connector.connect(uri)
    else:
        if getattr(args, 'unix_socket', None):
            path = '/tmp/sparrowrpc.sock'
            connector = AsyncUnixSocketConnector(engine, dispatcher)
            channel = await connector.connect(path)
        else:
            connector = AsyncTcpConnector(engine, dispatcher)
            channel = await connector.connect('127.0.0.1', 5000)
    await channel.start_channel()

    # start of test calls

    result = await channel.request(namespace='#sys').ping()
    print(f'Ping result: {result}')

    result = await channel.request.hello_world()
    print(result)

    result = await channel.request.hello_world(name='Bill')
    print(result)

    print()
    print('Requesting send and ack callbacks...')
    ack_requester = channel.request(msg_sent_callback=show_data, ack_callback=show_data)
    result = await ack_requester.hello_world()
    print(f'Got hello world result: {result}')

    print('Calling slow counter...')
    result = await channel.request.slow_counter(count_to=5, progress=show_progress)
    print(f'Got slow counter result: {result}')

    background_task = asyncio.create_task(background_counter(channel, 5, 1))

    print('Sleeping 10 on the main thread')
    await asyncio.sleep(10)

    print('Multipart response (returns a iterator)')
    proxy = channel.request(multipart_response=True)
    iterator = await proxy.multipart_response(count_to=10)
    async for x in iterator:
        print(x)
    print('Multipart response complete.')

    print('Sleeping 2 on the main thread')
    await asyncio.sleep(2)

    print('Iterable param (IterableCallback) - Pull')
    data = [1, 10, 30, 3]
    if micropython:
        # NOTE: MicroPython does not currently (May 2025) support async yield syntax, so have to return an async iterator 
        counter = AsyncData(data)
        result = await channel.request.iterable_param(nums=counter)
        print(f'Got iterable param result: {result}')
    else:
        async def iter_data():
            for x in data:
                yield x
        param = iter_data()
        result = await channel.request.iterable_param(nums=param)
        print(f'Got iterable param result: {result}')

    print('Sleeping 2 on the main thread')
    await asyncio.sleep(2)

    print('Waiting for backgroud task')
    await background_task

    # for (a, b) in [(1, 2), (1, 0), (3, 4), (11, 22), (None, 2), ('a', 'b')]:
    #     try:
    #         print()
    #         print(f'Calling division with a = {a}, b = {b}')
    #         result = channel.request.division(a=a, b=b)
    #         print(f'Got result: {result}')
    #     except CallerException as e:
    #         print(f'Opps - we made a mistake: {str(e)}')
    #     except CalleeException as e:
    #         print(f'Opps - something went wrong at the other end. {str(e)}')
    #     except Exception as e:
    #         print(f'Some other error - {str(e)}')

    await channel.shutdown_channel()
    await dispatcher.shutdown()

    print('Done')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')

    group = parser.add_mutually_exclusive_group() if not micropython else parser
    if MsgpackSerialiser:
        group.add_argument('--msgpack', action='store_true')
    if CborSerialiser:
        group.add_argument('--cbor', action='store_true')

    if not micropython:
        conn_group = parser.add_mutually_exclusive_group()
        conn_group.add_argument('--unix-socket', action='store_true')
        if AsyncWebsocketConnector:
            conn_group.add_argument('--websocket', action='store_true')
    args = parser.parse_args()

    root_logger = logging.getLogger()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
    asyncio.run(main(args))
