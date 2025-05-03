from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict, namedtuple
import json
import logging
import os
import socket
import sys
import asyncio
from asyncio import Lock, Event
try:
    from asyncio import Queue, QueueEmpty
except AttributeError:
    from uasync.queues import Queue, QueueEmpty
from traceback import format_exc
from typing import Any

from binarychain import BinaryChain, ChainReader


from ...core import ProtocolEngineBase, OutgoingRequest, OutgoingResponse, IncomingRequest, IncomingResponse
from ...engines import hs

from ...asyncio import AsyncMsgChannel, AsyncTransportBase


log = logging.getLogger(__name__)



class AsyncHandshake:
    BC_PREFIX = 'HS'
    SET_ENGINE = 'SET_ENGINE'
    REQUEST_CHOICES = 'REQUEST_CHOICES'

    def __init__(self, transport: AsyncTransportBase, initiator: bool, engine_choices: list[ProtocolEngineBase]):
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
        print(f'Handshake complete with engine {self.engine_selected}')
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
        return self._sync_send_and_receive(self.REQUEST_CHOICES)
    
    async def _sync_send_and_receive(self, target, **params):
        out_req = OutgoingRequest(target=target, params=params)
        message_id =  self.out_req_id
        self.out_req_id += 1

        bc = self.hs_engine.outgoing_message_to_binary_chain(out_req, message_id)
        bc.prefix = self.BC_PREFIX
        self.transport.send_binary_chain(bc)
        raw_response = await self.transport.incoming_queue.get()  # should be a binary chain
        assert isinstance(raw_response, BinaryChain)
        response = self.hs_engine.parse_incoming_message(raw_response)
        print(response)
        assert isinstance(response, IncomingResponse)
        assert response.request_id == message_id
        return response.result
    
    async def initator_set_engine(self, engine):
        # send engine choice to acceptor. Wait for response.
        assert isinstance(engine, ProtocolEngineBase)
        response = self._sync_send_and_receive(self.SET_ENGINE, choice=engine.get_engine_signature())
        print(f'Got set engine response: {response!r}')
        
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
            print(repr(in_req))
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
        raw_bc = await self.transport.incoming_queue.get()
        assert isinstance(raw_bc, BinaryChain)
        print(f'Handshake In: {repr(raw_bc)}')
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


# # Initiator always sends lists, the first entry being the 'command'
# # The acceptor always sends dicts back.
# class AsyncTcpHandshake:
#     BC_PREFIX = 'HS'
#     SET_ENGINE = 'SET_ENGINE'
#     REQUEST_CHOICES = 'REQUEST_CHOICES'
#     def __init__(self, conn_socket, initiator: bool, engine_choices: list[ProtocolEngineBase]):
#         assert isinstance(conn_socket, socket.socket)
#         self.conn_socket = conn_socket
#         self.initiator = initiator
#         self.engine_choices = engine_choices
#         self.engine_lookup = {engine.get_engine_signature(): engine for engine in self.engine_choices}
#         if not self.engine_choices:
#             raise ValueError('At least one engine choice must be passed in')
#         if len(self.engine_choices) != len(self.engine_lookup):
#             raise ValueError('Duplicate engine signatures')
#         self.engine_selected = None
#         self.handshake_complete = False
#     async def start_handshake(self):
#         # returns a protocol engine
#         if self.initiator:
#             await self._initator_handshake()
#         else:
#             await self._acceptor_handshake()
#         return self.engine_selected
#     async def _initator_handshake(self):
#         if len(self.engine_choices) == 1:
#             engine = self.engine_choices[0]
#         else:
#             offered_sigs = set(self.request_engine_choices())
#             for engine in self.engine_choices:
#                 if engine._sig in offered_sigs:
#                     break
#             else:
#                 raise RuntimeError('No compatible engine found')  # FIXME: better exception type
#         await self.initator_set_engine(engine)
#     async def _acceptor_handshake(self):
#         cmd_map = {self.SET_ENGINE: self.acceptor_set_engine,
#                    self.REQUEST_CHOICES: self.list_engine_choices,
#                    }
#         while not self.handshake_complete:
#             req = await self._get_handshake_msg()
#             print(repr(req))
#             if not isinstance(req, list):
#                 raise RuntimeError('Invalid handshake')
#             cmd = req[0]
#             args = req[1:]
#             print(f'Got cmd: {cmd}, args {args}')
#             cmd_func = cmd_map.get(cmd)
#             if cmd_func:
#                 try:
#                     cmd_func(*args)
#                 except Exception as e:
#                     msg = f'Handshake dispatch error: {str(e)}'
#                     log.error(msg)
#                     raise RuntimeError(msg)
#             else:
#                 raise RuntimeError(f'Invalid handshake command: {cmd}')
#     async def request_engine_choices(self):
#         cmd = [self.REQUEST_CHOICES]
#         return self._send_handshake_cmd(cmd)
#     async def initator_set_engine(self, engine):
#         # send engine choice to acceptor. Wait for response.
#         assert isinstance(engine, ProtocolEngineBase)
#         cmd = [self.SET_ENGINE, engine.get_engine_signature()]
#         response = await self._send_handshake_cmd(cmd)
#         if not isinstance(response, dict):
#             raise RuntimeError('Invalid response')
#         if 'accepted' in response:
#             # all good
#             self.engine_selected = engine
#             return
#         if 'rejected' in response:
#             msg = 'Engine {engine.sig} rejected'
#             log.error(msg)
#             log.info(f"Offered engine signatures: {response['rejected']}")
#             raise RuntimeError(msg)
#         raise RuntimeError('Invalid response')
#     async def acceptor_set_engine(self, *args):
#         choice = args[0]
#         if choice in self.engine_lookup.keys():
#             await self.accept_choice(choice)
#         else:
#             await self.reject_choice()
#     async def list_engine_choices(self):
#         await self._send_handshake_msg(dict(choices=self._get_sigs))
#     async def accept_choice(self, sig):
#         await self._send_handshake_msg(dict(accepted=True))
#         self.engine_selected = self.engine_lookup[sig]
#         self.handshake_complete = True
#     async def reject_choice(self):
#         await self._send_handshake_msg(dict(rejected=self._get_sigs()))
#         self.handshake_complete = True
#     async def _send_handshake_cmd(self, cmd):
#         await self._send_handshake_msg(cmd)
#         return self._get_handshake_msg()
#     async def _send_handshake_msg(self, msg):
#         bc = BinaryChain(self.BC_PREFIX, [json.dumps(msg).encode()])
#         print(f'Handshake Out: {repr(bc)}')
#         await self.conn_socket.sendall(bc.serialise())
#     async def _get_handshake_msg(self):
#         max_size = 1024  # way more than can possibly be needed for a handshake
#         chain_reader = ChainReader(max_size, max_size, max_chain_length=1)
#         while True:
#             data = await self.conn_socket.recv(1)
#             try:
#                 response = next(chain_reader.get_binary_chains(data))
#                 break
#             except StopIteration:
#                 continue
#         assert isinstance(response, BinaryChain)
#         print(f'Handshake In: {repr(response)}')
#         if response.prefix == self.BC_PREFIX:
#             return json.loads(response.parts[0])
#         else:
#             raise RuntimeError('Invalid prefix')
#     async def _get_sigs(self):
#         return [engine.get_engine_signature() for engine in self.engine_choices]


class AsyncTcpTransport(AsyncTransportBase):
    def __init__(self, conn_socket: socket.socket, max_msg_size=10*1024*1024, max_bc_length=10, incoming_msg_queue_size=10, outgoing_msg_queue_size=10, socket_buf_size=8192):
        AsyncTransportBase.__init__(self, max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, socket_buf_size)
        self.socket = conn_socket

    async def _read_data(self, size):
        return self.socket.recv(size)

    async def _write_data(self, data):
        log.debug(f'Sending data: {data}')
        self.socket.sendall(data)

    async def close(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()

    
class AsyncTcpConnector:
    def __init__(self, engine_choices, dispatcher, func_registers=None, handshake_cls=None):
        if isinstance(engine_choices, ProtocolEngineBase):
            self.engine_choices = [engine_choices]
        else:
            self.engine_choices = engine_choices
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.handshake_cls = handshake_cls if handshake_cls else AsyncHandshake
        self.initiator = True
    async def connect(self, host, port):
        conn_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        remote_address = (host, port)
        conn_socket.connect(remote_address)
        transport = AsyncTcpTransport(conn_socket)
        transport.start()
        handshake = AsyncHandshake(transport, self.initiator, self.engine_choices)
        #handshake = self.handshake_cls(conn_socket, self.initiator, self.engine_choices)
        handshake.start_handshake()
        if handshake.engine_selected:
            # transport = AsyncTcpTransport(handshake.engine_selected, conn_socket)  # FIX_ME: Allow options to be set / passed in??
            return AsyncMsgChannel(transport, initiator=self.initiator, engine=handshake.engine_selected, dispatcher=self.dispatcher, func_registers=self.func_registers)
        else:
            raise RuntimeError('No engine set')


class AsyncTcpListener:
    def __init__(self, engine_choices, dispatcher, func_registers=None, handshake_cls=None):
        if isinstance(engine_choices, ProtocolEngineBase):
            self.engine_choices = [engine_choices]
        else:
            self.engine_choices = engine_choices
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.handshake_cls = handshake_cls if handshake_cls else AsyncHandshake
        self.initiator = False
        self.tcp_server = None
        self.address = None
        self.channel_threads = dict()  # remote_address -> thread
        self.connected_channels = dict()  # remote_address -> channel
        self.time_to_stop = False

    async def run_server(self, bind_address, port):
        self.address = (bind_address, port)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(self.address)
        self.server_socket.listen(5)
        log.info(f'Listing on {self.address[0]}:{self.address[1]}')
        await asyncio.start_server(self._async_client_connected, sock=self.server_socket)

    async def _async_client_connected(self, async_reader, async_writer):
        channel_task = None

    async def shutdown_server(self):
        log.info('Starting Server Shutdown')
        self.time_to_stop = True
        for channel in self.connected_channels.values():
            assert isinstance(channel, AsyncMsgChannel)
            await channel.shutdown_channel()
        log.info('Server Shutdown Complete')

    async def _start_channel(self, client_socket, remote_address):
        transport = AsyncTcpTransport(client_socket)
        transport.start()
        handshake = self.handshake_cls(transport, self.initiator, self.engine_choices)
        handshake.start_handshake()
        if handshake.engine_selected:
            #transport = AsyncTcpTransport(client_socket)
            channel = AsyncMsgChannel(transport, initiator=False, engine=handshake.engine_selected, dispatcher=self.dispatcher, func_registers=self.func_registers)
            self.connected_channels[remote_address] = channel
            channel.start_channel()


class StreamTransport(AsyncTransportBase):
    def __init__(self, incoming_stream, outgoing_stream, max_msg_size=10*1024*1024, max_bc_length=10, incoming_msg_queue_size=10, outgoing_msg_queue_size=10, read_buf_size=8192):
        AsyncTransportBase.__init__(self, max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, read_buf_size)
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
        return AsyncMsgChannel(transport, initiator=True, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)


class ChildSubprocessRunner(SubprocessRunnerBase):
    def get_channel(self):
        transport = StreamTransport(sys.stdin.buffer, sys.stdout.buffer, self.engine)
        return AsyncMsgChannel(transport, initiator=False, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)
