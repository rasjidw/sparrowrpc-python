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
import signal
import sys
import tempfile

from threading import Thread, Lock, Event, current_thread
from queue import Queue, Empty as QueueEmpty
from traceback import format_exc
from typing import Any

if sys.platform != 'webassembly':
    import socket
else:
    socket = None

from binarychain import BinaryChain, ChainReader


from ...bases import ProtocolEngineBase
from ...engines import hs
from ...lib import SignalHandlerInstaller
from ...messages import IncomingRequest, IncomingResponse, OutgoingRequest, OutgoingResponse

from ...threaded import ThreadedMsgChannel, ThreadedTransportBase


log = logging.getLogger(__name__)



class ThreadedHandshake:
    BC_PREFIX = 'HS'
    SET_ENGINE = 'SET_ENGINE'
    REQUEST_CHOICES = 'REQUEST_CHOICES'

    def __init__(self, transport: ThreadedTransportBase, initiator: bool, engine_choices: list[ProtocolEngineBase]):
        self.transport = transport
        self.hs_engine = hs.ProtocolEngine(self.BC_PREFIX)
        self.initiator = initiator
        self.engine_choices = engine_choices
        self.engine_lookup = {engine.get_engine_signature(): engine for engine in self.engine_choices}
        if not self.engine_choices:
            raise ValueError('At least one engine choice must be passed in')
        if len(self.engine_choices) != len(self.engine_lookup):
            raise ValueError('Duplicate engine signatures')
        self.engine_selected = None
        self.handshake_complete = False
        self.out_req_id = 1
        self.out_resp_id = 1
        self.cmd_map = {self.SET_ENGINE: self.acceptor_set_engine,
                        self.REQUEST_CHOICES: self.list_engine_choices,
                        }

    def start_handshake(self):
        # returns a protocol engine
        if self.initiator:
            self._initator_handshake()
        else:
            self._acceptor_handshake()
        log.debug(f'Handshake complete with engine {self.engine_selected}')
        return self.engine_selected

    def _initator_handshake(self):
        if len(self.engine_choices) == 1:
            engine = self.engine_choices[0]
        else:
            offered_sigs = set(self.request_engine_choices())
            for engine in self.engine_choices:
                if engine._sig in offered_sigs:
                    break
            else:
                raise RuntimeError('No compatible engine found')  # FIXME: better exception type
        self.initator_set_engine(engine)
            
    def request_engine_choices(self):
        return self._sync_send_and_receive(self.REQUEST_CHOICES)
    
    def _sync_send_and_receive(self, target, **params):
        out_req = OutgoingRequest(target=target, params=params)
        message_id =  self.out_req_id
        self.out_req_id += 1

        bc = self.hs_engine.outgoing_message_to_binary_chain(out_req, message_id)
        bc.prefix = self.BC_PREFIX
        self.transport.send_binary_chain(bc)
        raw_response, complete, remote_closed = self.transport.incoming_queue.get()  # should be a binary chain
        assert isinstance(raw_response, BinaryChain)
        response = self.hs_engine.parse_incoming_message(raw_response)
        assert isinstance(response, IncomingResponse)
        assert response.request_id == message_id
        return response.result
    
    def initator_set_engine(self, engine):
        # send engine choice to acceptor. Wait for response.
        assert isinstance(engine, ProtocolEngineBase)
        response = self._sync_send_and_receive(self.SET_ENGINE, choice=engine.get_engine_signature())
        log.debug(f'Got set engine response: {response!r}')
        
        if 'accepted' in response:
            # all good
            self.engine_selected = engine
            self.handshake_complete = True
            return
        
        if 'rejected' in response:
            msg = 'Engine {engine.sig} rejected'
            log.error(msg)
            log.info(f"Offered engine signatures: {response['rejected']}")
            raise RuntimeError(msg)
        raise RuntimeError('Invalid response')

    def _acceptor_handshake(self):
        cmd_map = {self.SET_ENGINE: self.acceptor_set_engine,
                   self.REQUEST_CHOICES: self.list_engine_choices,
                   }
        while not self.handshake_complete:
            in_req = self._get_handshake_msg()
            assert isinstance(in_req, IncomingRequest)
            cmd_func = cmd_map.get(in_req.target)
            if cmd_func:
                try:
                    result = cmd_func(**in_req.params)
                    self._send_response(in_req, result)
                except Exception as e:
                    msg = f'Handshake dispatch error: {str(e)}'
                    log.error(msg)
                    raise RuntimeError(msg)
            else:
                raise RuntimeError(f'Invalid handshake command: {in_req.target}')
            
    def _send_response(self, in_req: IncomingRequest, result):
        id = self.out_resp_id
        self.out_resp_id += 1
        out_response = OutgoingResponse(result=result, request_id=in_req.id)
        out_bc = self.hs_engine.outgoing_message_to_binary_chain(out_response, message_id=id)
        out_bc.prefix = self.BC_PREFIX
        self.transport.send_binary_chain(out_bc)

    def acceptor_set_engine(self, choice):
        if choice in self.engine_lookup.keys():
            self.handshake_complete = True
            self.engine_selected = self.engine_lookup[choice]
            return dict(accepted=True)
        else:
            return dict(rejected=self._get_sigs())

    def list_engine_choices(self):
        return self._get_sigs()
    
    def _get_handshake_msg(self):
        raw_bc, complete, remote_closed = self.transport.incoming_queue.get()
        if raw_bc is None:
            raise RuntimeError('Connetion closed')  # FIXME - gracefully handle connectcion closures
        if raw_bc:
            assert isinstance(raw_bc, BinaryChain)
        if raw_bc.prefix == self.BC_PREFIX:
            incoming_msg = self.hs_engine.parse_incoming_message(raw_bc)
            if isinstance(incoming_msg, IncomingRequest):
                return incoming_msg
            else:
                raise RuntimeError('Invalid incoming handshake message')
        else:
            raise RuntimeError('Invalid prefix')
        
    def _get_sigs(self):
        return [engine.get_engine_signature() for engine in self.engine_choices]

class ThreadedTcpTransport(ThreadedTransportBase):
    def __init__(self, conn_socket: socket.socket, max_msg_size=10*1024*1024, max_bc_length=10, incoming_msg_queue_size=10, outgoing_msg_queue_size=10, socket_buf_size=8192):
        super().__init__(max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, socket_buf_size)
        self.socket = conn_socket

    def _read_data(self, size):
        return self.socket.recv(size)

    def _write_data(self, data):
        log.debug(f'Sending data: {data}')
        self.socket.sendall(data)

    def close(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()

    
class ThreadedTcpConnector:
    def __init__(self, engine_choices, dispatcher, func_registers=None, handshake_cls=None):
        if isinstance(engine_choices, ProtocolEngineBase):
            self.engine_choices = [engine_choices]
        else:
            self.engine_choices = engine_choices
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.handshake_cls = handshake_cls if handshake_cls else ThreadedHandshake
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
        handshake = self.handshake_cls(transport, self.initiator, self.engine_choices)
        handshake.start_handshake()
        if handshake.engine_selected:
            return ThreadedMsgChannel(transport, initiator=self.initiator, engine=handshake.engine_selected, dispatcher=self.dispatcher, func_registers=self.func_registers)
        else:
            raise RuntimeError('No engine set')


class ThreadedUnixSocketConnector(ThreadedTcpConnector):
    def __init__(self, engine_choices, dispatcher, func_registers=None, handshake_cls=None):
        super().__init__(engine_choices, dispatcher, func_registers, handshake_cls)

    def connect(self, path):
        return super().connect(path, None)


class ThreadedTcpListener:
    def __init__(self, engine_choices, dispatcher, func_registers=None, handshake_cls=None):
        if isinstance(engine_choices, ProtocolEngineBase):
            self.engine_choices = [engine_choices]
        else:
            self.engine_choices = engine_choices
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.handshake_cls = handshake_cls if handshake_cls else ThreadedHandshake
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

    def run_server(self, bind_address, port):
        self.listening_thread = Thread(target=self._run_server, args=(bind_address, port))
        self.listening_thread.start()

    def _run_server(self, bind_address, port):
        if port is None:
            self.address = bind_address
            self.unix_socket_path = bind_address
        else:
            self.address = f'{bind_address}:{port}'
        if self.unix_socket_path:
            if os.path.exists(self.unix_socket_path) and not self.replace_unix_socket_if_in_use:
                if self._detect_unix_socket_in_use(self.unix_socket_path):
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
        handshake = self.handshake_cls(transport, self.initiator, self.engine_choices)
        handshake.start_handshake()
        if handshake.engine_selected:
            #transport = ThreadedTcpTransport(client_socket)
            channel = ThreadedMsgChannel(transport, initiator=False, engine=handshake.engine_selected, dispatcher=self.dispatcher, func_registers=self.func_registers)
            self.connected_channels[remote_address] = channel
            channel.start_channel()

    def _signal_handler(self, signum, frame):
        signame = signal.Signals(signum).name
        log.info(f'Signal handler called with signal {signame} ({signum})')
        self.time_to_stop.set()

    def block(self, signals=None):
        signal_handler_installer = SignalHandlerInstaller(signals)
        signal_handler_installer.install(self._signal_handler)
        try:
            self.time_to_stop.wait()
            self.listening_thread.join()
        finally:
            signal_handler_installer.remove()
        self.shutdown_server()


class ThreadedUnixSocketListener(ThreadedTcpListener):
    def __init__(self, engine_choices, dispatcher, func_registers=None, handshake_cls=None):
        super().__init__(engine_choices, dispatcher, func_registers, handshake_cls)

    def run_server(self, path, replace_if_in_use=False):
        self.replace_unix_socket_if_in_use = replace_if_in_use
        super().run_server(path, None)


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
        assert isinstance(engine, ProtocolEngineBase)
        self.engine = engine
        self.dispatcher = dispatcher
        self.func_registers = func_registers

    @abstractmethod
    def get_channel(self, child_stdin=None, child_stdout=None):
        raise NotImplementedError()    


class ParentSubprocessRunner(SubprocessRunnerBase):
    def get_channel(self, child_stdin, child_stdout):
        transport = StreamTransport(outgoing_stream=child_stdin, incoming_stream=child_stdout, engine=self.engine)
        return ThreadedMsgChannel(transport, initiator=True, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)


class ChildSubprocessRunner(SubprocessRunnerBase):
    def get_channel(self):
        transport = StreamTransport(sys.stdin.buffer, sys.stdout.buffer, self.engine)
        return ThreadedMsgChannel(transport, initiator=False, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)
