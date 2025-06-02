from __future__ import annotations


import pytest

from listening_server_code import SOCK_PATH, MULTPART_RESPONSE_ITEMS

from sparrowrpc.serialisers import JsonSerialiser, MsgpackSerialiser, CborSerialiser
from sparrowrpc.engines import v050
from sparrowrpc.exceptions import CalleeException, CallerException

from sparrowrpc.threaded import ThreadedDispatcher
from sparrowrpc.threaded.transports import ThreadedUnixSocketConnector


@pytest.fixture(scope="module", params=[JsonSerialiser(), MsgpackSerialiser(), CborSerialiser()])
def channel(request):
    serialiser = request.param
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


def test_multipart_response(channel):
    items = list()
    for item in channel.request(multipart_response=True).multipart_response():
        items.append(item)
    assert items == MULTPART_RESPONSE_ITEMS


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
