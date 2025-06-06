from __future__ import annotations

from collections import namedtuple
import itertools
import os
import sys
from tempfile import NamedTemporaryFile
import time

import pytest
import pytest_asyncio


from listening_server_runner import ListeningServerRunner

import common_data

from sparrowrpc.serialisers import JsonSerialiser, MsgpackSerialiser, CborSerialiser
from sparrowrpc.engines import v050
from sparrowrpc.exceptions import CalleeException, CallerException

from sparrowrpc.asyncio import AsyncDispatcher




port_names = ['threaded_tcp', 'threaded_ws', 'threaded_uds', 'async_tcp', 'async_ws', 'async_uds']
Ports = namedtuple('Ports', port_names)


@pytest.fixture(scope="module")
def ports(start_port):
    if sys.platform == 'win32':
        threaded_socket_path = async_socket_path = None
    else:
        with NamedTemporaryFile(suffix='.sock', delete_on_close=False) as f:
            threaded_socket_path = f.name
        with NamedTemporaryFile(suffix='.sock', delete_on_close=False) as f:
            async_socket_path = f.name
    return Ports(start_port, start_port + 1, threaded_socket_path, start_port + 10, start_port + 11, async_socket_path)


@pytest.fixture(scope="module", autouse=True)
def run_listening_servers(ports: Ports):
    this_dir = os.path.dirname(os.path.abspath(__file__))
    threaded_server_code = os.path.join(this_dir, 'listening_server_code_threaded.py')
    args = ['--tcp_port', str(ports.threaded_tcp), '--ws_port', str(ports.threaded_ws)]
    if ports.threaded_uds:
        args.extend(['--uds_path', ports.threaded_uds])
    threaded_listening_server_runner = ListeningServerRunner(threaded_server_code, args)
    threaded_listening_server_runner.start()

    async_server_code = os.path.join(this_dir, 'listening_server_code_async.py')
    args = ['--tcp_port', str(ports.async_tcp), '--ws_port', str(ports.async_ws)]
    if ports.async_uds:
        args.extend(['--uds_path', ports.async_uds])
    async_listening_server_runner = ListeningServerRunner(async_server_code, args)
    async_listening_server_runner.start()

    time.sleep(2)  # wait for servers to fully start up    
    yield None
    threaded_listening_server_runner.stop()
    async_listening_server_runner.stop()


connect_to_list = port_names.copy()
if sys.platform != 'win32':
    connect_to_list.extend(['threaded_uds', 'async_uds'])

serialisers = [JsonSerialiser(), MsgpackSerialiser(), CborSerialiser()]

channel_params = list(itertools.product(connect_to_list, serialisers))


@pytest_asyncio.fixture(scope="module", params=channel_params)
async def channel(request, ports: Ports):
    connect_to, serialiser = request.param
    assert isinstance(connect_to, str)
    engine = v050.ProtocolEngine(serialiser)
    dispatcher = AsyncDispatcher(num_threads=5)
    if connect_to.endswith('tcp'):
        from sparrowrpc.asyncio.transports import AsyncTcpConnector
        tcp_connector = AsyncTcpConnector(engine, dispatcher)
        port = ports._asdict()[connect_to]
        channel = await tcp_connector.connect('127.0.0.1', port)
    elif connect_to.endswith('ws'):
        from sparrowrpc.asyncio.transports.websockets import AsyncWebsocketConnector
        ws_connector = AsyncWebsocketConnector(engine, dispatcher)
        port = ports._asdict()[connect_to]
        channel = await ws_connector.connect(f'ws://127.0.0.1:{port}/{engine.get_engine_signature()}')
    elif connect_to.endswith('uds'):
        from sparrowrpc.asyncio.transports import AsyncUnixSocketConnector
        uds_connector = AsyncUnixSocketConnector(engine, dispatcher)
        socket_path = ports._asdict()[connect_to]
        channel = await uds_connector.connect(socket_path)
    else:
        raise ValueError(f'Unexpected connect_to value of {connect_to}')
    await channel.start_channel()
    yield channel
    await channel.shutdown_channel()
    await dispatcher.shutdown()


@pytest.mark.asyncio(loop_scope="module")
async def test_ping(channel):
    result = await channel.request(namespace='#sys').ping()
    assert result == 'pong'


@pytest.mark.asyncio(loop_scope="module")
async def test_hello_world(channel):
    result = await channel.request.hello_world()
    assert result == 'Hello world!'


@pytest.mark.asyncio(loop_scope="module")
async def test_hello_name(channel):
    name = 'Bill'
    result = await channel.request.hello_world(name=name)
    assert result == f'Hello {name}!'


@pytest.mark.asyncio(loop_scope="module")
async def test_multipart_response(channel):
    items = list()
    proxy = channel.request(multipart_response=True)
    iterator = await proxy.multipart_response()
    async for item in iterator:
        items.append(item)
    assert items == common_data.MULTPART_RESPONSE_ITEMS


@pytest.mark.asyncio(loop_scope="module")
async def test_progress_callback(channel):
    progress_messages = list()
    async def progress(message):
        progress_messages.append(message)
    result = await channel.request.optional_progress_callback(progress=progress)
    assert result == sum(range(10))
    expected_messages = [f'Added {x} to total' for x in range(10)]
    assert expected_messages == progress_messages


@pytest.mark.asyncio(loop_scope="module")
async def test_progress_callback_not_used(channel):
    result = await channel.request.optional_progress_callback()
    assert result == sum(range(10))


@pytest.mark.asyncio(loop_scope="module")
async def test_iterable_param(channel):
    data = [1, 10, 30, 3]
    async def data_as_generator():
        for item in data:
            yield item
    iterable = data_as_generator()
    result = await channel.request.iterable_param(nums=iterable)
    assert result == sum(data)


@pytest.mark.asyncio(loop_scope="module")
async def test_division(channel):
    result = await channel.request.division(a=10, b=2)
    assert result == 5


@pytest.mark.asyncio(loop_scope="module")
async def test_invalid_param(channel):
    with pytest.raises(CallerException) as execinfo:
        result = await channel.request.division(a=10, b=None)


@pytest.mark.asyncio(loop_scope="module")
async def test_divide_zero(channel):
    with pytest.raises(CallerException) as execinfo:
        result = await channel.request.division(a=10, b=0)


@pytest.mark.asyncio(loop_scope="module")
async def test_callee_error(channel):
    with pytest.raises(CalleeException) as execinfo:
        result = await channel.request.division(a=11, b=2)
