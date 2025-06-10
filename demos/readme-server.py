#!/usr/bin/env python3

from random import randint
from time import sleep
from typing import Iterator

from sparrowrpc import export
from sparrowrpc.engines.v050 import ProtocolEngine
from sparrowrpc.serialisers import JsonSerialiser, MsgpackSerialiser, CborSerialiser

from sparrowrpc.threaded import ThreadedDispatcher, ThreadedMsgChannel, ThreadedMsgChannelInjector, ThreadedCallbackProxy
from sparrowrpc.threaded.transports import ThreadedTcpListener, ThreadedUnixSocketListener


@export
def hello_world(name: str = 'world'):
    return f'Hello {name}!'


@export
def demo_progress_update(data: list, progress: ThreadedCallbackProxy):
    progress.set_to_notification()
    item_count = 0
    for item in data:
        time_to_process = randint(5, 20) / 10
        sleep(time_to_process)  # do some work
        progress(message=f'Processed item {item} in {time_to_process} seconds.')
        item_count += 1
    return f'Processed {item_count} items'


@export(multipart_response=True)
def demo_multipart_response(up_to: int):
    for x in range(1, up_to + 1):
        yield (x, x**2 + 1)


@export
def demo_multipart_param(data: Iterator[int]):
    total = 0
    for x in data:
        total += x
    return total


def main():
    json_engine = ProtocolEngine(JsonSerialiser())
    msgpack_engine = ProtocolEngine(MsgpackSerialiser())
    cbor_engine = ProtocolEngine(CborSerialiser())
    engine_choicies = [json_engine, msgpack_engine, cbor_engine]

    dispatcher = ThreadedDispatcher(num_threads=5)    
    print('Running tcp server on 5000')
    tcp_server = ThreadedTcpListener(engine_choicies, dispatcher)
    tcp_server.run_server('0.0.0.0', 5000)
    dispatcher.shutdown()


if __name__ == '__main__':
    main()
