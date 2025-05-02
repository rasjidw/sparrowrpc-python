#!/usr/bin/env python3

import argparse
import asyncio
from collections import namedtuple
import logging
import sys
import time
try:
    from threading import current_thread
except ImportError:
    # micropython
    current_thread = lambda: namedtuple('cr', ['name'], defaults=['dummy'])


from sparrowrpc.core import make_export_decorator
from sparrowrpc.exceptions import CalleeException, CallerException
from sparrowrpc.serialisers import MsgpackSerialiser, JsonSerialiser
from sparrowrpc.core import IncomingResponse, IncomingException, OutgoingRequest, RequestCallbackInfo, FinalType, IterableCallbackInfo
from sparrowrpc.engines.v050 import ProtocolEngine

from sparrowrpc.asyncio import AsyncDispatcher
from sparrowrpc.asyncio.transports.websockets import AsyncWebsocketConnector


logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'               
                    )



export = make_export_decorator()

@export
async def display_chat_message(msg):
    print('********** Got incoming message **********')
    print(msg)                    
    print('**************************************')
    print()


async def show_progress(message):
    print(f'Running callback in {current_thread().name}')
    print(message)
    print('--------------')


async def show_data(data):
    print(f'Got data: {data}')


class ResultWaiter:
    def __init__(self):
        self.got_result = asyncio.Event()
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

    async def get_result(self):
        await self.got_result.wait()
        if self.exception:
            raise RuntimeError('Something bad happended')
        else:
            return self.result


async def background_counter(channel, count_to, delay):
    cb = lambda message: print(f'*** Got msg: {message}. Displaying in thread {current_thread().name}')
    proxy = await channel.get_proxy()
    print(f'*** Calling slow counter in background thread {current_thread().name}')
    req = OutgoingRequest('slow_counter', params=dict(count_to=count_to, delay=delay), callback_params={'progress': RequestCallbackInfo(cb)})
    result = await proxy.send_request(req)
    print(f'*** Background counter result: {result}')


async def main(use_msgpack):
    if use_msgpack:
        serialiser = MsgpackSerialiser()
    else:
        serialiser = JsonSerialiser()
    engine = ProtocolEngine(serialiser)
    dispatcher = AsyncDispatcher(num_threads=5)
    connector = AsyncWebsocketConnector(engine, dispatcher)
    engine_sig = engine.get_engine_signature()
    uri = f'ws://127.0.0.1:6000/{engine_sig}'
    channel = await connector.connect(ws_uri=uri)
    await channel.start_channel()
    
    proxy = await channel.get_proxy()

    req = OutgoingRequest('ping', namespace='#sys')
    result = await proxy.send_request(req)
    print(f'Got result: {result}')

    req = OutgoingRequest('hello_world')
    req.acknowledge = True
    result = await proxy.send_request(req, msg_sent_callback=show_data, ack_callback=show_data)
    print(f'Got result: {result}')

    name = 'Fred'
    req = OutgoingRequest('hello_world', params={'name': name})
    result = await proxy.send_request(req)
    print(f'Got result: {result}')

    req = OutgoingRequest('slow_counter', params={'count_to': 5}, callback_params={'progress': RequestCallbackInfo(show_progress)})
    result = await proxy.send_request(req)
    print(f'Got result: {result}')


    # background_thread = threading.Thread(target=background_counter, args=(channel, 5, 1))
    # background_thread.start()

    # print('Sleeping 2 on the main thread')
    # time.sleep(2)

    # print('Calling multipart_response')
    # req = OutgoingRequest('multipart_response', params={'count_to': 10})
    # for x in proxy.send_request_multipart_result_as_generator(req):
    #     print(x)

    # print('Sleeping 2 on the main thread')
    # time.sleep(2)

    # print('Iterable param (IterableCallback) - Pull')
    # iter_data = iter([1, 2, 3, 4])
    # req = OutgoingRequest('iterable_param', callback_params={'nums': IterableCallbackInfo(iter_data)})
    # result = proxy.send_request(req)
    # print(f'Got result: {result}')

    # print('Sleeping 2 on the main thread')
    # time.sleep(2)

    # print('Waiting for backgroud thread')
    # background_thread.join()

    # for (a, b) in [(1, 2), (1, 0), (3, 4), (11, 22), (None, 2), ('a', 'b')]:
    #     try:
    #         print()
    #         print(f'Calling division with a = {a}, b = {b}')
    #         result = proxy.send_request(OutgoingRequest('division', params=dict(a=a, b=b)))
    #         print(f'Got result: {result}')
    #     except CallerException as e:
    #         print(f'Opps - we made a mistake: {str(e)}')
    #     except CalleeException as e:
    #         print(f'Opps - something went wrong at the other end. {str(e)}')
    #     except Exception as e:
    #         print(f'Some other error - {str(e)}')

    await channel.shutdown_channel()
    print('Channel shut down')
    await dispatcher.shutdown()

    print('Done')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--msgpack', action='store_true')
    args = parser.parse_args()

    root_logger = logging.getLogger()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
    asyncio.run(main(args.msgpack))
