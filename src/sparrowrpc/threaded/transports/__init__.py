# -----------------------------------------------------------------------
# WARNING: This file is auto-generated and should not be edited manually!
#
# It is generated using 'generate_from_template_code.py' using the
# _template_ directory as the source.
# -----------------------------------------------------------------------

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict, namedtuple
import logging
import os
import tempfile
import sys

from threading import Thread, Lock, Event, current_thread
from queue import Queue, Empty as QueueEmpty
from traceback import format_exc
from typing import Any

if sys.platform != 'webassembly':
    import socket
else:
    socket = None

running_micropython = (sys.implementation.name == 'micropython')

if not running_micropython:
    import signal


from binarychain import BinaryChain, ChainReader


from ...engine import ProtocolEngine
from ...encoders import hs
if not running_micropython:
    from ...lib import SignalHandlerInstaller, detect_unix_socket_in_use
from ...messages import Request, Response, TransportClosedEvent, TransportBrokenEvent

from ...threaded import ThreadedMsgChannel, ThreadedTransportBase


log = logging.getLogger(__name__)



class ThreadedTcpTransport(ThreadedTransportBase):
    def __init__(self, conn_socket: socket.socket, max_msg_size=10*1024*1024, max_bc_length=10, incoming_msg_queue_size=10, outgoing_msg_queue_size=10, socket_buf_size=8192):
        super().__init__(max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, socket_buf_size)
        self.socket = conn_socket
        self.closing_socket = False

    def _read_data(self, size):
        try:
            data = self.socket.recv(size)
            if not data:
                return TransportClosedEvent('remote')
            else:
                return data
        except Exception as e:
            if self.closing_socket:
                # windows and micropython return errors once the socket is closed
                # just return 
                return TransportClosedEvent('local')
            else:
                return TransportBrokenEvent(e)

    def _write_data(self, data):
        log.debug(f'Sending data: {data}')
        self.socket.sendall(data)

    def close(self):
        self.closing_socket = True
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()

    
class ThreadedTcpConnector:
    def __init__(self, engine: ProtocolEngine, dispatcher, func_registers=None):
        self.engine = engine
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.initiator = True
        self.unix_socket_path = None
        
    def connect(self, host, port):
        if port is None:
            self.unix_socket_path = host
        if self.unix_socket_path:
            conn_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            conn_socket.connect(self.unix_socket_path)
        else:
            conn_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            remote_address = (host, port)
            conn_socket.connect(remote_address)
        transport = ThreadedTcpTransport(conn_socket)
        transport.start()
        return ThreadedMsgChannel(transport, initiator=self.initiator, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)


class ThreadedUnixSocketConnector(ThreadedTcpConnector):
    def __init__(self, engine_choices, dispatcher, func_registers=None, handshake_cls=None):
        super().__init__(engine_choices, dispatcher, func_registers)

    def connect(self, path):
        return super().connect(path, None)


class ThreadedTcpListener:
    def __init__(self, engine: ProtocolEngine, dispatcher, func_registers=None):
        self.engine = engine
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.initiator = False
        self.tcp_server = None
        self.address = None
        self.channel_threads = dict()  # remote_address -> thread
        self.connected_channels = dict()  # remote_address -> channel
        self.listening_thread = None
        self.time_to_stop = Event()
        self.unix_socket_path = None
        self.replace_unix_socket_if_in_use = False
        self.server_socket = None
        self.async_server = None

    def _detect_unix_socket_in_use(self, socket_path):
        detecting_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            detecting_socket.connect(socket_path)
            detecting_socket.close()
            return True
        except Exception:
            return False

    def run_server(self, bind_address, port, block=True):
        self.listening_thread = Thread(target=self._run_server, args=(bind_address, port))
        self.listening_thread.start()
        if block:
            self.block()

    def _run_server(self, bind_address, port):
        if port is None:
            self.address = bind_address
            self.unix_socket_path = bind_address
        else:
            self.address = f'{bind_address}:{port}'
        if self.unix_socket_path:
            if os.path.exists(self.unix_socket_path) and not self.replace_unix_socket_if_in_use:
                if detect_unix_socket_in_use(self.unix_socket_path):
                    raise OSError(f'Unix Socket {self.unix_socket_path} in use')
                
            # get a temporary name in the same directory as the final path
            socket_dir = os.path.dirname(self.unix_socket_path)
            with tempfile.NamedTemporaryFile(dir=socket_dir, delete=True) as f:
                temp_uds = f.name
            self.server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.server_socket.bind(temp_uds)

            # renames are atomic on unix / linux
            os.rename(temp_uds, self.unix_socket_path)
        else:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            address_and_port = (bind_address, port)
            self.server_socket.bind(address_and_port)
        self.server_socket.listen(5)
        self.server_socket.settimeout(1)
        log.info(f'Listing on {self.address}')
        while not self.time_to_stop.is_set():
            try:
                client_socket, remote_address = self.server_socket.accept()
            except socket.timeout:
                continue

            log.info(f'Accepted connection from {remote_address}')
            transport = ThreadedTcpTransport(client_socket)
            channel_thread = Thread(target=self._start_channel, args=(transport, remote_address))
            self.channel_threads[remote_address] = channel_thread
            channel_thread.start()

    def shutdown_server(self):
        log.info('Starting Server Shutdown')
        self.time_to_stop.set()
        self.listening_thread.join()
        for channel in self.connected_channels.values():
            assert isinstance(channel, ThreadedMsgChannel)
            channel.shutdown_channel()
        for channel_thread in self.channel_threads.values():
            assert isinstance(channel_thread, Thread)
            channel_thread.join()
        self.server_socket.close()
        # cleanup unix socket if we are not replacing by default
        if self.unix_socket_path and not self.replace_unix_socket_if_in_use:
            os.remove(self.unix_socket_path)
        log.info('Server Shutdown Complete')

    def _start_channel(self, transport: ThreadedTcpTransport, remote_address):
        transport.start()
        channel = ThreadedMsgChannel(transport, initiator=False, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)
        self.connected_channels[remote_address] = channel
        channel.start_channel()

    def _signal_handler(self, signum, frame):
        signame = signal.Signals(signum).name
        log.info(f'Signal handler called with signal {signame} ({signum})')
        self.time_to_stop.set()

    def block(self, signals=None):
        signal_handler_installer = None
        if not running_micropython:
            signal_handler_installer = SignalHandlerInstaller(signals)
            signal_handler_installer.install(self._signal_handler)
        try:
            while True:
                # the timeout is required for Windows to enable the signals to be caught
                if self.time_to_stop.wait(timeout=0.5):
                    break
            self.listening_thread.join()
        finally:
            if signal_handler_installer:
                signal_handler_installer.remove()
        self.shutdown_server()


class ThreadedUnixSocketListener(ThreadedTcpListener):
    def __init__(self, engine_choices, dispatcher, func_registers=None):
        super().__init__(engine_choices, dispatcher, func_registers)

    def run_server(self, path, replace_if_in_use=False, block=True):
        self.replace_unix_socket_if_in_use = replace_if_in_use
        super().run_server(path, None, block)


class StreamTransport(ThreadedTransportBase):
    def __init__(self, incoming_stream, outgoing_stream, max_msg_size=10*1024*1024, max_bc_length=10, incoming_msg_queue_size=10, outgoing_msg_queue_size=10, read_buf_size=8192):
        super().__init__(max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, read_buf_size)
        self.incoming_stream = incoming_stream
        self.outgoing_stream = outgoing_stream

    def _read_data(self, size):
        try:
            return self.incoming_stream.read1(size)  # use read1 so we don't block waiting for 'more' data
        except Exception as e:
            log.debug(f'Error reading from incoming stream in PID {os.getpid()}: {str(e)}')
            return None
    
    def _write_data(self, data):
        # log.debug(f'Wrote to outgoing stream: {data!s}')
        self.outgoing_stream.write(data)
        self.outgoing_stream.flush()

    def close(self):
        self.incoming_stream.close()
        self.outgoing_stream.close()


class SubprocessRunnerBase(ABC):
    def __init__(self, engine, dispatcher, func_registers=None):
        assert isinstance(engine, ProtocolEngine)
        self.engine = engine
        self.dispatcher = dispatcher
        self.func_registers = func_registers

    @abstractmethod
    def get_channel(self, child_stdin=None, child_stdout=None):
        raise NotImplementedError()    


class ParentSubprocessRunner(SubprocessRunnerBase):
    def get_channel(self, child_stdin, child_stdout):
        transport = StreamTransport(outgoing_stream=child_stdin, incoming_stream=child_stdout)
        return ThreadedMsgChannel(transport, initiator=True, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)


class ChildSubprocessRunner(SubprocessRunnerBase):
    def get_channel(self):
        transport = StreamTransport(sys.stdin.buffer, sys.stdout.buffer)
        return ThreadedMsgChannel(transport, initiator=False, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)
