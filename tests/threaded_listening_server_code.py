
import logging
import sys
import threading

from sparrowrpc import export, InvalidParams
from sparrowrpc.engines.v050 import ProtocolEngine
from sparrowrpc.serialisers import MsgpackSerialiser, JsonSerialiser, MsgpackSerialiser, CborSerialiser

from sparrowrpc.threaded import ThreadedDispatcher, ThreadedMsgChannel, ThreadedMsgChannelInjector, ThreadedCallbackProxy
from sparrowrpc.threaded.transports import ThreadedUnixSocketListener


SOCK_PATH = '/tmp/sparrowrpc-testing.sock'

MULTPART_RESPONSE_ITEMS = ['apple', 'bannana', 'mango']


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
                    )


@export
def hello_world(name=None):
    if name:
        return f'Hello {name}!'
    else:
        return f'Hello world!'


@export(multipart_response=True)
def multipart_response():
    for fruit in MULTPART_RESPONSE_ITEMS:
        yield fruit


@export
def optional_progress_callback(progress: ThreadedCallbackProxy = None):
    if progress:
        progress.set_to_notification()
    total = 0
    for x in range(10):
        total += x
        if progress:
            progress(message=f'Added {x} to total')
    return total


@export(injectable_params=dict(channel=ThreadedMsgChannelInjector))
def iterable_param(nums, channel):
    assert isinstance(channel, ThreadedMsgChannel)
    total = 0
    for x in nums:
        total += x
    return total


@export
def division(a, b):
    try:
        result = a / b
    except (TypeError, ValueError) as e:
        raise InvalidParams(f'Invalid param type: {str(e)}')
    except ZeroDivisionError:
        raise InvalidParams('b must not be 0')
    if a == 11:
        raise RuntimeError('a == 11 is a server bug')
    return result


def main():
    json_engine = ProtocolEngine(JsonSerialiser())
    msgpack_engine = ProtocolEngine(MsgpackSerialiser())
    cbor_engine = ProtocolEngine(CborSerialiser())

    engine_choicies = [json_engine, msgpack_engine, cbor_engine]
    dispatcher = ThreadedDispatcher(num_threads=5)    
    print(f'Listening on unix socket {SOCK_PATH}')
    uds_server = ThreadedUnixSocketListener(engine_choicies, dispatcher)
    uds_server.run_server(SOCK_PATH)
    dispatcher.shutdown()
    print('Server stopped.')

    if threading.active_count():
        print('Threads still running:')
        for thread in threading.enumerate():
            print(f' {thread.name}')


if __name__ == '__main__':
    main()
