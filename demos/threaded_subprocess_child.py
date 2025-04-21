#!/usr/bin/env python3

import argparse
import logging
import sys
from time import sleep
from threading import current_thread

from lucido.core import make_export_decorator
from lucido.serialisers import MsgpackSerialiser, JsonSerialiser
from lucido.threaded import ThreadedDispatcher, ThreadedCallbackProxy
from lucido.engines.v050 import ProtocolEngine
from lucido.threaded.transports import ChildSubprocessRunner


logging.basicConfig(stream=sys.stderr, level=logging.INFO,
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
def slow_counter(count_to: int, delay: int = 0.5, progress: ThreadedCallbackProxy = None):
    if progress:
        progress.set_to_notification()
    for x in range(count_to):
        msg = f'Counted to {x} in thread - {current_thread().name}'
        if progress:
            progress(message=msg)
        sleep(delay)
    result = x + 1
    return f'Counted to {result} with a delay of {delay} between counts. All done.'



def main(use_msgpack):
    sys.stderr.write('*** CHILD STARTING ***\n')
    sys.stderr.flush()

    json_engine = ProtocolEngine(JsonSerialiser())
    msgpack_engine = ProtocolEngine(MsgpackSerialiser())
    engine = msgpack_engine if use_msgpack else json_engine
    
    dispatcher = ThreadedDispatcher(num_threads=5)
    server = ChildSubprocessRunner(engine, dispatcher)
    channel = server.get_channel()
    channel.start_channel()
    channel.wait_for_remote_close()
    dispatcher.shutdown()

    sys.stderr.write('*** CHILD DONE ***\n')
    sys.stderr.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--msgpack', action='store_true')
    args = parser.parse_args()

    root_logger = logging.getLogger()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
    main(args.msgpack)
