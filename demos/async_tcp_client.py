#!/usr/bin/env python3

import argparse
import asyncio
import logging
import sys

from sparrowrpc.core import make_export_decorator, IncomingResponse, IncomingException
from sparrowrpc.serialisers import MsgpackSerialiser, JsonSerialiser
from sparrowrpc.engines.v050 import ProtocolEngine

from sparrowrpc.asyncio import AsyncDispatcher
from sparrowrpc.asyncio.transports import AsyncTcpConnector
try:
    from sparrowrpc.asyncio.transports.websockets import AsyncWebsocketConnector
except ImportError:
    AsyncWebsocketConnector = None


logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')


def get_thread_or_task_name():
    return asyncio.current_task().get_name()


export = make_export_decorator()

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


async def main(use_msgpack, use_websocket):
    if use_msgpack:
        serialiser = MsgpackSerialiser()
    else:
        serialiser = JsonSerialiser()
    engine = ProtocolEngine(serialiser)
    dispatcher = AsyncDispatcher(num_threads=5)
    if use_websocket:
        connector = AsyncWebsocketConnector(engine, dispatcher)
        engine_sig = engine.get_engine_signature()
        uri = f'ws://127.0.0.1:6000/{engine_sig}'
        channel = await connector.connect(uri)
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
    print(f'Got result: {result}')

    print('Calling slow counter...')
    result = await channel.request.slow_counter(count_to=5, progress=show_progress)
    print(f'Got result: {result}')

    background_task = asyncio.create_task(background_counter(channel, 5, 1))

    print('Sleeping 2 on the main thread')
    await asyncio.sleep(2)

    print('Multipart response (returns a generator)')
    proxy = channel.request(multipart_response=True)
    generator = proxy.multipart_response(count_to=10)
    async for x in generator:
        print(x)
    print('Multipart response complete.')

    print('Sleeping 2 on the main thread')
    await asyncio.sleep(2)

    print('Iterable param (IterableCallback) - Pull')
    async def iter_data():
        for x in [1, 10, 30, 3]:
            yield x
    param = iter_data()
    print(dir(param))
    result = await channel.request.iterable_param(nums=param)
    print(f'Got result: {result}')

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
    parser.add_argument('--msgpack', action='store_true')
    parser.add_argument('--websocket', action='store_true')
    args = parser.parse_args()

    root_logger = logging.getLogger()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
    asyncio.run(main(args.msgpack, args.websocket))
