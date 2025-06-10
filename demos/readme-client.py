#!/usr/bin/env python3

from sparrowrpc.serialisers import JsonSerialiser, MsgpackSerialiser, CborSerialiser
from sparrowrpc.engines.v050 import ProtocolEngine
from sparrowrpc.threaded import ThreadedDispatcher
from sparrowrpc.threaded.transports import ThreadedTcpConnector


def show_progress(message):
    print(message)


serialiser = JsonSerialiser()
# serialiser = MsgpackSerialiser()  # or this
# serialiser = CborSerialiser()  # or this
engine = ProtocolEngine(serialiser)

dispatcher = ThreadedDispatcher(num_threads=5)

connector = ThreadedTcpConnector(engine, dispatcher)
channel = connector.connect('127.0.0.1', 5000)
channel.start_channel()


result = channel.request(namespace='#sys').ping()
print(result)

result = channel.request.hello_world()
print(result)

result = channel.request.hello_world(name='Bill')
print(result)

data = ['apple', 'pear', 'bannana']
result = channel.request.demo_progress_update(data=data, progress=show_progress)
print(result)

result = list()
for x in channel.request(multipart_response=True).demo_multipart_response(up_to=10):
    result.append(x)
print(result)

iter_data = iter([1, 10, 30, 3])
result = channel.request.demo_multipart_param(data=iter_data)
print(result)

channel.shutdown_channel()
dispatcher.shutdown()
