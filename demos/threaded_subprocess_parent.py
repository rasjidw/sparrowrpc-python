#!/usr/bin/env python3

import argparse
import logging
import os
import pathlib
import subprocess
import sys
import threading
import time

from sparrowrpc.decorators import make_export_decorator
from sparrowrpc.threaded import ThreadedDispatcher
from sparrowrpc.engine import ProtocolEngine
from sparrowrpc.threaded.transports import ParentSubprocessRunner


logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')


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


def log_child_stderr(stderr):
    print('** Reading from stderr')
    line_data = b''
    while True:
        data = stderr.read(1)
        if not data:
            break
        line_data += data
        if line_data.decode()[-1] == '\n':
            print(f'** Got child stderr: {line_data.decode()}')
            line_data = b''
    print('** Stderr closed')


def main(debug, use_msgpack):
    engine = ProtocolEngine()
    dispatcher = ThreadedDispatcher(num_threads=5)

    transport = ParentSubprocessRunner(engine, dispatcher)
    demo_dir = pathlib.Path(__file__).parent
    args = [demo_dir / 'threaded_subprocess_child.py']
    if debug:
        args.append('--debug')
    if use_msgpack:
        args.append('--msgpack')

    print(f'Parent PID is: {os.getpid()}')

    print(f'Starting subprocess with args: {args}')
    child_proc = subprocess.Popen(args, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    stderr_reading_thread = threading.Thread(target=log_child_stderr, args=(child_proc.stderr,))
    stderr_reading_thread.daemon = True
    stderr_reading_thread.start()

    print(f'Child PID is: {child_proc.pid}')

    print(child_proc.stdin, dir(child_proc.stdin), child_proc.stdin.readable())
    print(child_proc.stdout, dir(child_proc.stdout), child_proc.stdout.writable())
    print(child_proc.returncode)

    print('Starting channel...')
    channel = transport.get_channel(child_stdin=child_proc.stdin, child_stdout=child_proc.stdout)
    channel.start_channel()

    # start of test calls

    result = channel.request(namespace='#sys').ping()
    print(f'Ping result: {result}')

    result = channel.request.hello_world()
    print(result)

    result = channel.request.hello_world(name='Bill')
    print(result)

    print()
    print('Requesting send and ack callbacks...')
    ack_requester = channel.request(msg_sent_callback=show_data, ack_callback=show_data)
    result = ack_requester.hello_world()
    print(f'Got result: {result}')

    print('Calling slow counter...')
    result = channel.request.slow_counter(count_to=5, progress=show_progress)
    print(f'Got result: {result}')

    child_proc.stdin.close()

    channel.shutdown_channel()
    dispatcher.shutdown()

    rc = child_proc.wait()
    print(f'Got exit code: {rc} from child')

    print('Done')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--msgpack', action='store_true')
    args = parser.parse_args()

    root_logger = logging.getLogger()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
    main(args.debug, args.msgpack)
