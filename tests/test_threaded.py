from __future__ import annotations

from collections import namedtuple
import itertools
import sys
from tempfile import NamedTemporaryFile

import pytest


import threaded_listening_server_code
from listening_server_runner import ListeningServerRunner

import common_data

from sparrowrpc.serialisers import JsonSerialiser, MsgpackSerialiser, CborSerialiser
from sparrowrpc.engines import v050
from sparrowrpc.exceptions import CalleeException, CallerException

from sparrowrpc.threaded import ThreadedDispatcher

if sys.platform == 'win32':
    socket_path = None
else:
    with NamedTemporaryFile(suffix='.sock', delete_on_close=False) as f:
        socket_path = f.name


port_names = ['threaded_tcp', 'threaded_ws', 'async_tcp', 'async_ws']
Ports = namedtuple('Ports', port_names)


@pytest.fixture(scope="module")
def ports(start_port):
    return Ports(start_port, start_port + 1, start_port + 10, start_port + 11)


@pytest.fixture(scope="module", autouse=True)
def threaded_server(ports: Ports):
    args = ['--tcp_port', str(ports.threaded_tcp), '--ws_port', str(ports.threaded_ws)]
    if socket_path:
        args.extend(['--uds_path', socket_path])
    listening_server_runner = ListeningServerRunner(threaded_listening_server_code.__file__, args)
    listening_server_runner.start()
    yield None
    listening_server_runner.stop()


connect_to_list = port_names.copy()[:1]  # FIXME: not done async server side yet, and websocket not working...
if sys.platform != 'win32':
    connect_to_list.append('uds')

serialisers = [JsonSerialiser(), MsgpackSerialiser(), CborSerialiser()]

channel_params = list(itertools.product(connect_to_list, serialisers))


@pytest.fixture(scope="module", params=channel_params)
def channel(request, ports: Ports):
    connect_to, serialiser = request.param
    assert isinstance(connect_to, str)
    engine = v050.ProtocolEngine(serialiser)
    dispatcher = ThreadedDispatcher(num_threads=5)
    if connect_to.endswith('tcp'):
        from sparrowrpc.threaded.transports import ThreadedTcpConnector
        tcp_connector = ThreadedTcpConnector(engine, dispatcher)
        port = ports._asdict()[connect_to]
        channel = tcp_connector.connect('127.0.0.1', port)
    elif connect_to.endswith('ws'):
        from sparrowrpc.threaded.transports.websockets import ThreadedWebsocketConnector
        ws_connector = ThreadedWebsocketConnector(engine, dispatcher)
        port = ports._asdict()[connect_to]
        channel = ws_connector.connect(f'ws://127.0.0.1:{port}/{engine.get_engine_signature()}')
    elif connect_to == 'uds':
        from sparrowrpc.threaded.transports import ThreadedUnixSocketConnector
        uds_connector = ThreadedUnixSocketConnector(engine, dispatcher)
        channel = uds_connector.connect(socket_path)
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


def test_multipart_response(channel):
    items = list()
    for item in channel.request(multipart_response=True).multipart_response():
        items.append(item)
    assert items == common_data.MULTPART_RESPONSE_ITEMS


def test_progress_callback(channel):
    progress_messages = list()
    def progress(message):
        progress_messages.append(message)
    result = channel.request.optional_progress_callback(progress=progress)
    assert result == sum(range(10))
    expected_messages = [f'Added {x} to total' for x in range(10)]
    assert expected_messages == progress_messages


def test_progress_callback_not_used(channel):
    result = channel.request.optional_progress_callback()
    assert result == sum(range(10))


def test_iterable_param(channel):
    data = [1, 10, 30, 3]
    def data_as_generator():
        for item in data:
            yield item
    iterable = data_as_generator()
    result = channel.request.iterable_param(nums=iterable)
    assert result == sum(data)


def test_division(channel):
    result = channel.request.division(a=10, b=2)
    assert result == 5


def test_invalid_param(channel):
    with pytest.raises(CallerException) as execinfo:
        result = channel.request.division(a=10, b=None)


def test_divide_zero(channel):
    with pytest.raises(CallerException) as execinfo:
        result = channel.request.division(a=10, b=0)


def test_callee_error(channel):
    with pytest.raises(CalleeException) as execinfo:
        result = channel.request.division(a=11, b=2)
