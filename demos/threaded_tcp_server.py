#!/usr/bin/env python3

import argparse
import logging
import sys
from time import sleep
from threading import current_thread

from lucido.core import ProtocolEngine, MsgpackSerialiser, JsonSerialiser, CallbackProxy, make_export_decorator, MsgChannelInjector
from lucido.exceptions import InvalidParams
from lucido.threaded import TcpListener, ThreadPoolDispatcher, MsgChannel
from lucido.threaded.websockets import WebsocketListener


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
                    )


export = make_export_decorator()


@export
def hello_world(name=None):
    if name:
        return f'Hello {name}!'
    else:
        return f'Hello world!'


@export
def slow_counter(count_to: int, delay: int = 0.5, progress: CallbackProxy = None):
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

@export(injectable_params=dict(channel=MsgChannelInjector))
def iterable_param(nums, channel):
    assert isinstance(channel, MsgChannel)
    count = 0
    for x in nums:
        msg = f'Fetched {x} from remote end'
        print(f'>>>>>>>>>>>>>> {msg} <<<<<<<<<<<<<')
        channel.request.display_chat_message(msg=msg)
        count += x
    return count


@export(multipart_request='nums')
def multipart_request(nums, start=0):
    print(f'In multipart request with start of {start}')
    sum = start
    for x in nums:
        print(f'Adding {x}')
        sum += x
    return sum


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
    parser.add_argument('--websocket', action='store_true')
    args = parser.parse_args()

    json_engine = ProtocolEngine(JsonSerialiser())
    msgpack_engine = ProtocolEngine(MsgpackSerialiser())
    engine_choicies = [msgpack_engine, json_engine]
    
    dispatcher = ThreadPoolDispatcher(num_threads=5)
    if args.websocket:
        print('Running websocket server on 6000')
        websocket_server = WebsocketListener(engine_choicies, dispatcher)
        websocket_server.run_server('0.0.0.0', 6000)
    else:
        print('Running tcp server on 5000')
        tcp_server = TcpListener(engine_choicies, dispatcher)
        tcp_server.run_server('0.0.0.0', 5000)
    dispatcher.shutdown()


if __name__ == '__main__':
    main()
