from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict, namedtuple
import json
import logging
import os
import socket
import sys
if 'threaded' in __name__: #= remove
    from threading import Thread, Lock, Event, current_thread  #= threaded <
    from queue import Queue, Empty as QueueEmpty  #= threaded <
else: #= remove
    import asyncio  #= async <
    from asyncio import Lock, Event  #= async <
    try: #= async <
        from asyncio import Queue, QueueEmpty #= async <
    except AttributeError:  #= async <
        from uasync.queues import Queue, QueueEmpty  #= async <
from traceback import format_exc
from typing import Any

from binarychain import BinaryChain, ChainReader


from ...core import ProtocolEngineBase, OutgoingRequest, OutgoingResponse, IncomingRequest, IncomingResponse
from ...engines import hs

from ..._template_ import _Template_MsgChannel, _Template_TransportBase


log = logging.getLogger(__name__)



class _Template_Handshake:
    BC_PREFIX = 'HS'
    SET_ENGINE = 'SET_ENGINE'
    REQUEST_CHOICES = 'REQUEST_CHOICES'

    def __init__(self, transport: _Template_TransportBase, initiator: bool, engine_choices: list[ProtocolEngineBase]):
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

    async def start_handshake(self):
        # returns a protocol engine
        if self.initiator:
            await self._initator_handshake()
        else:
            await self._acceptor_handshake()
        log.debug(f'Handshake complete with engine {self.engine_selected}')
        return self.engine_selected

    async def _initator_handshake(self):
        if len(self.engine_choices) == 1:
            engine = self.engine_choices[0]
        else:
            offered_sigs = set(self.request_engine_choices())
            for engine in self.engine_choices:
                if engine._sig in offered_sigs:
                    break
            else:
                raise RuntimeError('No compatible engine found')  # FIXME: better exception type
        await self.initator_set_engine(engine)
            
    async def request_engine_choices(self):
        return await self._sync_send_and_receive(self.REQUEST_CHOICES)
    
    async def _sync_send_and_receive(self, target, **params):
        out_req = OutgoingRequest(target=target, params=params)
        message_id =  self.out_req_id
        self.out_req_id += 1

        bc = self.hs_engine.outgoing_message_to_binary_chain(out_req, message_id)
        bc.prefix = self.BC_PREFIX
        await self.transport.send_binary_chain(bc)
        raw_response, complete, remote_closed = await self.transport.incoming_queue.get()  # should be a binary chain
        assert isinstance(raw_response, BinaryChain)
        response = self.hs_engine.parse_incoming_message(raw_response)
        assert isinstance(response, IncomingResponse)
        assert response.request_id == message_id
        return response.result
    
    async def initator_set_engine(self, engine):
        # send engine choice to acceptor. Wait for response.
        assert isinstance(engine, ProtocolEngineBase)
        response = await self._sync_send_and_receive(self.SET_ENGINE, choice=engine.get_engine_signature())
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

    async def _acceptor_handshake(self):
        cmd_map = {self.SET_ENGINE: self.acceptor_set_engine,
                   self.REQUEST_CHOICES: self.list_engine_choices,
                   }
        while not self.handshake_complete:
            in_req = await self._get_handshake_msg()
            assert isinstance(in_req, IncomingRequest)
            cmd_func = cmd_map.get(in_req.target)
            if cmd_func:
                try:
                    result = await cmd_func(**in_req.params)
                    await self._send_response(in_req, result)
                except Exception as e:
                    msg = f'Handshake dispatch error: {str(e)}'
                    log.error(msg)
                    raise RuntimeError(msg)
            else:
                raise RuntimeError(f'Invalid handshake command: {in_req.target}')
            
    async def _send_response(self, in_req: IncomingRequest, result):
        id = self.out_resp_id
        self.out_resp_id += 1
        out_response = OutgoingResponse(result=result, request_id=in_req.id)
        out_bc = self.hs_engine.outgoing_message_to_binary_chain(out_response, message_id=id)
        out_bc.prefix = self.BC_PREFIX
        await self.transport.send_binary_chain(out_bc)

    async def acceptor_set_engine(self, choice):
        if choice in self.engine_lookup.keys():
            self.handshake_complete = True
            self.engine_selected = self.engine_lookup[choice]
            return dict(accepted=True)
        else:
            return dict(rejected=self._get_sigs())

    async def list_engine_choices(self):
        return self._get_sigs()
    
    async def _get_handshake_msg(self):
        raw_bc, complete, remote_closed = await self.transport.incoming_queue.get()
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
        
    async def _get_sigs(self):
        return [engine.get_engine_signature() for engine in self.engine_choices]

#= threaded start
class _Template_TcpTransport(_Template_TransportBase):
    def __init__(self, conn_socket: socket.socket, max_msg_size=10*1024*1024, max_bc_length=10, incoming_msg_queue_size=10, outgoing_msg_queue_size=10, socket_buf_size=8192):
        _Template_TransportBase.__init__(self, max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, socket_buf_size)
        self.socket = conn_socket

    def _read_data(self, size):
        return self.socket.recv(size)

    def _write_data(self, data):
        log.debug(f'Sending data: {data}')
        self.socket.sendall(data)

    def close(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
#= threaded end

#= async start
class _Template_TcpAsyncTransport(_Template_TransportBase):
    def __init__(self, stream_reader: asyncio.StreamReader, stream_writer: asyncio.StreamWriter, max_msg_size=10*1024*1024, max_bc_length=10, incoming_msg_queue_size=10, outgoing_msg_queue_size=10, socket_buf_size=8192):
        _Template_TransportBase.__init__(self, max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, socket_buf_size)
        self.stream_reader = stream_reader
        self.stream_writer = stream_writer

    async def _read_data(self, size):
        return await self.stream_reader.read(size)

    async def _write_data(self, data):
        log.debug(f'Sending data: {data}')
        self.stream_writer.write(data)
        await self.stream_writer.drain()

    async def close(self):
        self.stream_writer.close()
        await self.stream_writer.wait_closed()
#= async end
    
class _Template_TcpConnector:
    def __init__(self, engine_choices, dispatcher, func_registers=None, handshake_cls=None):
        if isinstance(engine_choices, ProtocolEngineBase):
            self.engine_choices = [engine_choices]
        else:
            self.engine_choices = engine_choices
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.handshake_cls = handshake_cls if handshake_cls else _Template_Handshake
        self.initiator = True
        
    async def connect(self, host, port):
        #= threaded start
        conn_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        remote_address = (host, port)
        conn_socket.connect(remote_address)
        transport = _Template_TcpTransport(conn_socket)
        #= threaded end
        #= async start
        reader, writer = await asyncio.open_connection(host, port)
        transport = _Template_TcpAsyncTransport(reader, writer)
        #= async end
        await transport.start()
        handshake = self.handshake_cls(transport, self.initiator, self.engine_choices)
        await handshake.start_handshake()
        if handshake.engine_selected:
            return _Template_MsgChannel(transport, initiator=self.initiator, engine=handshake.engine_selected, dispatcher=self.dispatcher, func_registers=self.func_registers)
        else:
            raise RuntimeError('No engine set')


class _Template_TcpListener:
    def __init__(self, engine_choices, dispatcher, func_registers=None, handshake_cls=None):
        if isinstance(engine_choices, ProtocolEngineBase):
            self.engine_choices = [engine_choices]
        else:
            self.engine_choices = engine_choices
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.handshake_cls = handshake_cls if handshake_cls else _Template_Handshake
        self.initiator = False
        self.tcp_server = None
        self.address = None
        self.channel_threads = dict()  # remote_address -> thread  #= threaded
        self.channel_tasks = dict()  # remote_address -> task  #= async
        self.connected_channels = dict()  # remote_address -> channel
        self.time_to_stop = False

    async def run_server(self, bind_address, port):
        self.address = (bind_address, port)
        #= threaded start
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(self.address)
        self.server_socket.listen(5)
        log.info(f'Listing on {self.address[0]}:{self.address[1]}')
        try:
            while not self.time_to_stop:
                client_socket, remote_address = self.server_socket.accept()
                log.info(f'Accepted connection from {remote_address}')
                transport = _Template_TcpTransport(client_socket)
                channel_thread = Thread(target=self._start_channel, args=(transport, remote_address))
                self.channel_threads[remote_address] = channel_thread
                channel_thread.start()
        except KeyboardInterrupt:
            self.shutdown_server()
        #= threaded end
        #= async start
        if sys.implementation.name == 'micropython':
            server = await asyncio.start_server(self._async_client_connected, bind_address, port)
            await server.wait_closed()
        else:
            server = await asyncio.start_server(self._async_client_connected, bind_address, port, reuse_address=True)
            try:
                log.info(f'Listing on {self.address[0]}:{self.address[1]}')
                await server.serve_forever()
            except KeyboardInterrupt:
                await self.shutdown_server()
        #= async end

    #= async start
    async def _async_client_connected(self, async_reader: asyncio.StreamReader, async_writer: asyncio.StreamWriter):
        # FIXME: work out what the peername format is in micropython
        remote_address = repr(async_writer.get_extra_info('peername'))
        log.info(f'REMOTE ADDRESS: {remote_address}')
        transport = _Template_TcpAsyncTransport(async_reader, async_writer)
        channel_task = asyncio.create_task(self._start_channel(transport, remote_address))
        self.channel_tasks[remote_address] = channel_task

    #= async end
    async def shutdown_server(self):
        log.info('Starting Server Shutdown')
        self.time_to_stop = True
        for channel in self.connected_channels.values():
            assert isinstance(channel, _Template_MsgChannel)
            await channel.shutdown_channel()
        #= threaded start
        for channel_thread in self.channel_threads.values():
            assert isinstance(channel_thread, Thread)
            channel_thread.join()
        self.server_socket.close()
        #= threaded end
        log.info('Server Shutdown Complete')

    async def _start_channel(self, transport: _Template_TcpTransport, remote_address):
        await transport.start()
        handshake = self.handshake_cls(transport, self.initiator, self.engine_choices)
        await handshake.start_handshake()
        if handshake.engine_selected:
            #transport = _Template_TcpTransport(client_socket)
            channel = _Template_MsgChannel(transport, initiator=False, engine=handshake.engine_selected, dispatcher=self.dispatcher, func_registers=self.func_registers)
            self.connected_channels[remote_address] = channel
            await channel.start_channel()


class StreamTransport(_Template_TransportBase):
    def __init__(self, incoming_stream, outgoing_stream, max_msg_size=10*1024*1024, max_bc_length=10, incoming_msg_queue_size=10, outgoing_msg_queue_size=10, read_buf_size=8192):
        _Template_TransportBase.__init__(self, max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, read_buf_size)
        self.incoming_stream = incoming_stream
        self.outgoing_stream = outgoing_stream

    async def _read_data(self, size):
        try:
            return self.incoming_stream.read1(size)  # use read1 so we don't block waiting for 'more' data
        except Exception as e:
            log.debug(f'Error reading from incoming stream in PID {os.getpid()}: {str(e)}')
            return None
    
    async def _write_data(self, data):
        # log.debug(f'Wrote to outgoing stream: {data!s}')
        self.outgoing_stream.write(data)
        self.outgoing_stream.flush()

    async def close(self):
        self.incoming_stream.close()
        self.outgoing_stream.close()


class SubprocessRunnerBase(ABC):
    def __init__(self, engine, dispatcher, func_registers=None):
        assert isinstance(engine, ProtocolEngineBase)
        self.engine = engine
        self.dispatcher = dispatcher
        self.func_registers = func_registers

    @abstractmethod
    async def get_channel(self, child_stdin=None, child_stdout=None):
        raise NotImplementedError()    


class ParentSubprocessRunner(SubprocessRunnerBase):
    async def get_channel(self, child_stdin, child_stdout):
        transport = StreamTransport(outgoing_stream=child_stdin, incoming_stream=child_stdout, engine=self.engine)
        return _Template_MsgChannel(transport, initiator=True, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)


class ChildSubprocessRunner(SubprocessRunnerBase):
    def get_channel(self):
        transport = StreamTransport(sys.stdin.buffer, sys.stdout.buffer, self.engine)
        return _Template_MsgChannel(transport, initiator=False, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)
