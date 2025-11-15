#!/usr/bin/env python3

from sparrowrpc.engine import ProtocolEngine
from sparrowrpc.threaded import ThreadedDispatcher
from sparrowrpc.threaded.transports import ThreadedTcpConnector


def show_progress(message):
    print(message)


engine = ProtocolEngine()
dispatcher = ThreadedDispatcher(num_threads=5)
connector = ThreadedTcpConnector(engine, dispatcher)
channel = connector.connect('127.0.0.1', 5000)
channel.start_channel()

result = channel.request(namespace='#sys').ping()
print(result)
# result = 'pong'

result = channel.request.hello_world()
print(result)
# result = 'Hello world!'

result = channel.request.hello_world(name='Bill')
print(result)
# result = 'Hello Bill!'

data = ['apple', 'pear', 'banana']
result = channel.request.demo_progress_update(data=data, progress=show_progress)
# displays:
# Processed item apple in 1.7 seconds.
# Processed item pear in 1.0 seconds.
# Processed item banana in 0.8 seconds.
print(result)
# result = 'Processed 3 items'

result = list()
for x in channel.request(multipart_response=True).demo_multipart_response(up_to=10):
    result.append(x)
print(result)
# result = [[1, 2], [2, 5], [3, 10], [4, 17], [5, 26], [6, 37], [7, 50], [8, 65], [9, 82], [10, 101]]

print('Iterable param (IterableCallback) - Pull')
iter_data = iter([1, 10, 30, 3])
result = channel.request.demo_multipart_param(data=iter_data)
print(result)
# result = 44

channel.shutdown_channel()
dispatcher.shutdown()
