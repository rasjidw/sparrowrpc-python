from __future__ import annotations

from time import sleep
from typing import List

import pytest

from listening_server_code import SOCK_PATH

from sparrowrpc.lib import detect_unix_socket_in_use
from sparrowrpc.serialisers import JsonSerialiser
from sparrowrpc.engines import v050

from sparrowrpc.threaded import ThreadedDispatcher
from sparrowrpc.threaded.transports import ThreadedUnixSocketConnector


@pytest.fixture(scope="module")
def channel():
    sleep(2)
    # wait for up to 5 seconds for the socket to become active
    # for _ in range(10):
    #     if detect_unix_socket_in_use(SOCK_PATH):
    #         break
    #     else:
    #         print('SLEEPING....')
    #         sleep(0.5)
    # else:
    #     raise RuntimeError('Socket not active')
    
    serialiser = JsonSerialiser()
    engine = v050.ProtocolEngine(serialiser)
    dispatcher = ThreadedDispatcher(num_threads=5)
    connector = ThreadedUnixSocketConnector(engine, dispatcher)
    channel = connector.connect(SOCK_PATH)
    channel.start_channel()
    yield channel
    channel.shutdown_channel()
    dispatcher.shutdown()


def test_ping(channel):
    result = channel.request(namespace='#sys').ping()
    assert result == 'pong'

def test_hello_world(channel):
    result = channel.request.hello_world()
    assert result == 'Hello world!'

def test_hello_name(channel):
    name = 'Bill'
    result = channel.request.hello_world(name=name)
    assert result == f'Hello {name}!'
