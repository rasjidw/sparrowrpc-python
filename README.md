# Sparrow RPC

Sparrow PRC is a language independent, lightweight Event Based Multi-Transport Peer-to-Peer bidirectional RPC System and Protocol Engine.

The purpose of Sparrow is to not just be a RPC system, but to also be a high level api for the creation of
sophisticated protocols without worrying about 'bytes on the wire'.


## Core Features

* Transport Agnostic including TCP sockets, Websockets, Stdin/Stdout and Unix Domain Sockets.
* Serialisation Agnostic. Currently supports Json, MessagePack and CBOR out of the box.
* Fully Peer-to-Peer - both sides of any connection are equal.
* RPC requests can flow both directions, and can be interleaved with each other.
* RPC calls support multipart responses (ie, can return an iterable).
* RPC calls can include remote callbacks.
* Pub / Sub style of calls is also supported.
* Low network overhead when paired with a compact serialisation library like MessagePack or CBOR.
* Efficient binary data transfer even when using a serialisation library that does not support binary data.


## Platforms

The Python version of Sparrow RPC runs on Python 3.8+, including PyPy and GraalPy.
It also runs on MicroPython (currently only tested on the unix port of MicroPython) and under PyScript.


## Status

### Sparrow RPC is in heavy development and is currently ALPHA.

Most of the core features are done but expect breaking changes at this stage, and a lot of error handling is not yet in place.


## Example usages

Keeping in mind that both sides can export functions, with the only difference being that one side listens for incoming connections and the other side initiates the connection, here we refer to the 'listening' side as the 'server' and the initating side as the 'client'.

**Server code:**

```python
#!/usr/bin/env python3

from random import randint
from time import sleep
from typing import Iterator

from sparrowrpc import export
from sparrowrpc.engine import ProtocolEngine
from sparrowrpc.threaded import ThreadedDispatcher, ThreadedCallbackProxy
from sparrowrpc.threaded.transports import ThreadedTcpListener


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
    engine = ProtocolEngine()
    dispatcher = ThreadedDispatcher(num_threads=5)
    print('Running tcp server on 5000')
    tcp_server = ThreadedTcpListener(engine, dispatcher)
    tcp_server.run_server('0.0.0.0', 5000)
    dispatcher.shutdown()


if __name__ == '__main__':
    main()
```

Client side, run in the python REPL, you have:

```python
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
```
