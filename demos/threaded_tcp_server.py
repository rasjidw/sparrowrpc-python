#!/usr/bin/env python3

import argparse
import logging
import sys
from time import sleep
from threading import current_thread

from sparrowrpc import export, InvalidParams
from sparrowrpc.engines.v050 import ProtocolEngine
from sparrowrpc.serialisers import JsonSerialiser
try:
    from sparrowrpc.serialisers import MsgpackSerialiser
except ImportError:
     MsgpackSerialiser = None
try:
    from sparrowrpc.serialisers import CborSerialiser
except ImportError:
    CborSerialiser = None

from sparrowrpc.threaded import ThreadedDispatcher, ThreadedMsgChannel, ThreadedMsgChannelInjector, ThreadedCallbackProxy
from sparrowrpc.threaded.transports import ThreadedTcpListener, ThreadedUnixSocketListener
try:
    from sparrowrpc.threaded.transports.websockets import ThreadedWebsocketListener
except ImportError:
    ThreadedWebsocketListener = None


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
                    )


@export
def hello_world(name=None):
    if name:
        return f'Hello {name}!'
    else:
        return f'Hello world!'


@export
def slow_counter(count_to: int, delay: int = 0.5, progress: ThreadedCallbackProxy = None):
    if progress:
        progress.set_to_notification()
    for x in range(count_to):
        msg = f'Counted to {x} in thread - {current_thread().name}'
        if progress:
            progress(message=msg)
        sleep(delay)
    result = x + 1
    return f'Counted to {result} with a delay of {delay} between counts. All done.'


@export(multipart_response=True)
def multipart_response(count_to: int):
    for x in range(count_to):
        yield (x, x+1)


@export(injectable_params=dict(channel=ThreadedMsgChannelInjector))
def iterable_param(nums, channel):
    assert isinstance(channel, ThreadedMsgChannel)
    count = 0
    for x in nums:
        msg = f'Fetched {x} from remote end'
        print(f'>>>>>>>>>>>>>> {msg} <<<<<<<<<<<<<')
        channel.request.display_chat_message(msg=msg)
        count += x
    return count


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
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--unix-socket', action='store_true')
    if ThreadedWebsocketListener:
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

    dispatcher = ThreadedDispatcher(num_threads=5)    
    if getattr(args, 'websocket', False):
        print('Running websocket server on 9001')
        websocket_server = ThreadedWebsocketListener(engine_choicies, dispatcher)
        websocket_server.run_server('0.0.0.0', 9001)
        websocket_server.block()
    else:
        if args.unix_socket:
            path = '/tmp/sparrowrpc.sock'
            print(f'Listening on unix socket {path}')
            socket_server = ThreadedUnixSocketListener(engine_choicies, dispatcher)
            socket_server.run_server(path)
            socket_server.block()
        else:
            print('Running tcp server on 5000')
            tcp_server = ThreadedTcpListener(engine_choicies, dispatcher)
            tcp_server.run_server('0.0.0.0', 5000)
            tcp_server.block()
    dispatcher.shutdown()


if __name__ == '__main__':
    main()
