#!/usr/bin/env python3

import argparse
import logging
import sys
import threading
import time
from traceback import print_exc


from sparrowrpc.decorators import make_export_decorator
from sparrowrpc.exceptions import CalleeException, CallerException
from sparrowrpc.messages import FinalType, ExceptionResponse, Response, Request, RequestCallbackInfo
from sparrowrpc.serialisers import MsgpackSerialiser, JsonSerialiser
from sparrowrpc.messages import IterableCallbackInfo
from sparrowrpc.engine import ProtocolEngine

from sparrowrpc.threaded import ThreadedDispatcher
from sparrowrpc.threaded.transports import ThreadedTcpConnector


logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'               
                    )



export = make_export_decorator()

@export
def display_chat_message(msg):
    print('********** Got incoming message **********')
    print(msg)                    
    print('**************************************')
    print()


def show_progress(message):
    print(f'Running callback in {threading.current_thread().name}')
    print(message)
    print('--------------')


def show_data(data):
    print(f'Got data: {data}')


class ResultWaiter:
    def __init__(self):
        self.got_result = threading.Event()
        self.messages = []
        self.result = None
        self.exception = None

    def process_msg(self, msg):
        show_data(msg)
        if isinstance(msg, Response):
            self.result = msg.result
            self.got_result.set()
        elif isinstance(msg, ExceptionResponse):
            self.exception = msg.exc_info
            self.got_result.set()
        else:
            self.messages.append(msg)

    def get_result(self):
        self.got_result.wait()
        if self.exception:
            raise RuntimeError('Something bad happened')
        else:
            return self.result


def background_counter(channel, count_to, delay):
    cb = lambda message: print(f'*** Got msg: {message}. Displaying in thread {threading.current_thread().name}')
    proxy = channel.get_proxy()
    print(f'*** Calling slow counter in background thread {threading.current_thread().name}')
    req = Request('slow_counter', params=dict(count_to=count_to, delay=delay), callback_params={'progress': RequestCallbackInfo(cb)})
    result = proxy.send_request(req)
    print(f'*** Background counter result: {result}')


def main(use_msgpack):
    if use_msgpack:
        serialiser = MsgpackSerialiser()
    else:
        serialiser = JsonSerialiser()

    engine = ProtocolEngine()
    dispatcher = ThreadedDispatcher(num_threads=5)
    connector = ThreadedTcpConnector(engine, dispatcher)
    channel = connector.connect('127.0.0.1', 5000)
    channel.start_channel()
    
    proxy = channel.get_proxy()

    req = Request('ping', namespace='#sys')
    result = proxy.send_request(req)
    print(f'Got result: {result}')

    req = Request('hello_world')
    req.acknowledge = True
    result = proxy.send_request(req, msg_sent_callback=show_data, ack_callback=show_data)
    print(f'Got result: {result}')

    name = 'Fred'
    req = Request('hello_world', params={'name': name})
    result = proxy.send_request(req)
    print(f'Got result: {result}')

    req = Request('slow_counter', params={'count_to': 5}, callback_params={'progress': RequestCallbackInfo(show_progress)})
    result = proxy.send_request(req)
    print(f'Got result: {result}')


    background_thread = threading.Thread(target=background_counter, args=(channel, 5, 1))
    background_thread.start()

    print('Sleeping 2 on the main thread')
    time.sleep(2)

    print('Calling multipart_response')
    req = Request('multipart_response', params={'count_to': 10})
    for x in proxy.send_request_multipart_result_as_iterator(req):
        print(x)

    print('Sleeping 2 on the main thread')
    time.sleep(2)

    print('Iterable param (IterableCallback) - Pull')
    iter_data = iter([1, 2, 3, 4])
    req = Request('iterable_param', callback_params={'nums': IterableCallbackInfo(iter_data)})
    result = proxy.send_request(req)
    print(f'Got result: {result}')

    print('Sleeping 2 on the main thread')
    time.sleep(2)

    print('Waiting for background thread')
    background_thread.join()

    for (a, b) in [(1, 2), (1, 0), (3, 4), (11, 22), (None, 2), ('a', 'b')]:
        try:
            print()
            print(f'Calling division with a = {a}, b = {b}')
            result = proxy.send_request(Request('division', params=dict(a=a, b=b)))
            print(f'Got result: {result}')
        except CallerException as e:
            print(f'Opps - we made a mistake: {str(e)}')
        except CalleeException as e:
            print(f'Opps - something went wrong at the other end. {str(e)}')
        except Exception as e:
            print(f'Some other error - {str(e)}')

    channel.shutdown_channel()
    dispatcher.shutdown()

    print('Done')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--msgpack', action='store_true')
    args = parser.parse_args()

    root_logger = logging.getLogger()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
    main(args.msgpack)
