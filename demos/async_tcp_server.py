#!/usr/bin/env python3

import argparse
import asyncio
import logging
import sys

from sparrowrpc import export, InvalidParams
from sparrowrpc.engines.v050 import ProtocolEngine
from sparrowrpc.serialisers import MsgpackSerialiser
from sparrowrpc.serialisers import JsonSerialiser
try:
    from sparrowrpc.serialisers import MsgpackSerialiser
except ImportError:
     MsgpackSerialiser = None
try:
    from sparrowrpc.serialisers import CborSerialiser
except ImportError:
    CborSerialiser = None

from sparrowrpc.asyncio import AsyncDispatcher, AsyncMsgChannel, AsyncMsgChannelInjector, AsyncCallbackProxy
from sparrowrpc.asyncio.transports import AsyncTcpListener, AsyncUnixSocketListener
try:
    from sparrowrpc.asyncio.transports.websockets import AsyncWebsocketListener
except ImportError:
    AsyncWebsocketListener = None


if sys.implementation.name == 'micropython':
    print('**** MICROPYTHON ****')
    logging.basicConfig(level=logging.DEBUG)
    micropython = True
else:
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
    micropython = False


def get_thread_or_task_name():
    name_getter = getattr(asyncio.current_task(), 'get_name', None)
    if name_getter:
        return name_getter()
    else:
        return 'dummy'


@export
async def hello_world(name=None):
    if name:
        return f'Hello {name}!'
    else:
        return f'Hello world!'


@export
async def slow_counter(count_to: int, delay: int = 0.5, progress: AsyncCallbackProxy = None):
    if progress:
        progress.set_to_notification()
    for x in range(count_to):
        msg = f'Counted to {x} in thread - {get_thread_or_task_name()}'
        if progress:
            await progress(message=msg)
        await asyncio.sleep(delay)
    result = x + 1
    return f'Counted to {result} with a delay of {delay} between counts. All done.'


if micropython:
    # NOTE: MicroPython does not currently (May 2025) support async yield syntax, so have to return an async iterator 
    class AsyncCounter:
        def __init__(self, count_to):
            self.count_to = count_to
            self.current_count = 0
        def __aiter__(self):
            return self
        async def __anext__(self):
            if self.current_count >= self.count_to:
                raise StopAsyncIteration()
            result = (self.current_count, self.current_count + 1)
            self.current_count += 1
            return result

    @export(multipart_response=True)
    async def multipart_response(count_to: int):
        return AsyncCounter(count_to)

else:
    @export(multipart_response=True)
    async def multipart_response(count_to: int):
        for x in range(count_to):
            yield (x, x+1)


@export(injectable_params=dict(channel=AsyncMsgChannelInjector))
async def iterable_param(nums, channel):
    print('************* In iterable_param *************')
    print(type(channel))
    assert isinstance(channel, AsyncMsgChannel)
    count = 0
    async for x in nums:
        msg = f'Fetched {x} from remote end'
        print(f'>>>>>>>>>>>>>> {msg} <<<<<<<<<<<<<')
        await channel.request.display_chat_message(msg=msg)
        count += x
    return count


@export
async def division(a, b):
    try:
        result = a / b
    except (TypeError, ValueError) as e:
        raise InvalidParams(f'Invalid param type: {str(e)}')
    except ZeroDivisionError:
        raise InvalidParams('b must not be 0')
    if a == 11:
        raise RuntimeError('a == 11 is a bug')
    return result



async def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group() if not micropython else parser
    group.add_argument('--unix-socket', action='store_true')
    if AsyncWebsocketListener:
        group.add_argument('--websocket', action='store_true')
    args = parser.parse_args()

    json_engine = ProtocolEngine(JsonSerialiser())
    engine_choicies = [json_engine]
    if MsgpackSerialiser:
        msgpack_engine = ProtocolEngine(MsgpackSerialiser())
        engine_choicies.append(msgpack_engine)
    if CborSerialiser:
        cbor_engine = ProtocolEngine(CborSerialiser())
        engine_choicies.append(cbor_engine)

    print(f'Engine Serialisation options are: {[e.get_engine_signature() for e in engine_choicies]}')
    
    dispatcher = AsyncDispatcher(num_threads=5)
    if getattr(args, 'websocket', False):
        print('Running websocket server on 9001')
        websocket_server = AsyncWebsocketListener(engine_choicies, dispatcher)
        await websocket_server.run_server('0.0.0.0', 9001)
    else:
        if args.unix_socket:
            path = '/tmp/sparrowrpc.sock'
            print(f'Listening on unix socket {path}')
            socket_server = AsyncUnixSocketListener(engine_choicies, dispatcher)
            await socket_server.run_server(path)
        else:
            print('Running tcp server on 5000')
            tcp_server = AsyncTcpListener(engine_choicies, dispatcher)
            await tcp_server.run_server('0.0.0.0', 5000)
    await dispatcher.shutdown()


if __name__ == '__main__':
    asyncio.run(main())
