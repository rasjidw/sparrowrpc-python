#!/usr/bin/env python3

import argparse
import asyncio
import datetime
import logging
import sys
import time

from sparrowrpc.engine import ProtocolEngine
import sparrowrpc.serialisers

from sparrowrpc.asyncio import AsyncDispatcher
from sparrowrpc.asyncio.transports import AsyncTcpConnector, AsyncUnixSocketConnector
try:
    from sparrowrpc.asyncio.transports.websockets import AsyncWebsocketConnector
except ImportError:
    AsyncWebsocketConnector = None


if sys.implementation.name == 'micropython':
    print('**** MICROPYTHON ****')
    logging.basicConfig(level=logging.ERROR)
    micropython = True
else:
    logging.basicConfig(stream=sys.stdout, level=logging.ERROR, format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
    micropython = False




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
        default_serialiser_sig = 'm'
    elif args.cbor:
        default_serialiser_sig = 'c'
    else:
        default_serialiser_sig = 'j'
    host = args.host
    engine = ProtocolEngine()
    dispatcher = AsyncDispatcher(num_threads=5)
    if getattr(args, 'websocket', None):
        connector = AsyncWebsocketConnector(engine, dispatcher, default_serialiser_sig=default_serialiser_sig)
        port = 9001 if not args.port else args.port
        uri = f'ws://{host}:{port}/'
        channel = await connector.connect(uri)
    else:
        if getattr(args, 'unix_socket', None):
            path = '/tmp/sparrowrpc.sock'
            connector = AsyncUnixSocketConnector(engine, dispatcher, default_serialiser_sig=default_serialiser_sig)
            channel = await connector.connect(path)
        else:
            connector = AsyncTcpConnector(engine, dispatcher, default_serialiser_sig=default_serialiser_sig)
            port = 5000 if not args.port else args.port
            channel = await connector.connect(host, port)
    await channel.start_channel()

    # start of test calls

    count = 10000
    ping_start = time.perf_counter()
    for _ in range(count):
        await channel.request(namespace='#sys').ping()
    ping_stop = time.perf_counter()
    pings_per_second = count / (ping_stop - ping_start)
    print(f'Pings per second: {pings_per_second}')

    hello_start = time.perf_counter()
    for _ in range(count):
        await channel.request.hello_world(name='Bill')
    hello_stop = time.perf_counter()
    hellos_per_second = count / (hello_stop - hello_start)
    print(f'Hellos per second: {hellos_per_second}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')

    group = parser.add_mutually_exclusive_group() if not micropython else parser
    if sparrowrpc.serialisers.MsgpackSerialiser:
        group.add_argument('--msgpack', action='store_true')
    if sparrowrpc.serialisers.CborSerialiser:
        group.add_argument('--cbor', action='store_true')

    if not micropython:
        conn_group = parser.add_mutually_exclusive_group()
        conn_group.add_argument('--unix-socket', action='store_true')
        if AsyncWebsocketConnector:
            conn_group.add_argument('--websocket', action='store_true')

    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=0)
    args = parser.parse_args()

    root_logger = logging.getLogger()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
    asyncio.run(main(args))
