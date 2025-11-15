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

import asyncio
from asyncio import Lock, Event
try:
    from asyncio import Queue, QueueEmpty
except (AttributeError, ImportError):
    from uasync.queues import Queue, QueueEmpty # type: ignore (for micropython)
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

from ...asyncio import AsyncMsgChannel, AsyncTransportBase


log = logging.getLogger(__name__)




class AsyncTcpTransport(AsyncTransportBase):
    def __init__(self, stream_reader: asyncio.StreamReader, stream_writer: asyncio.StreamWriter, max_msg_size=10*1024*1024, max_bc_length=10, incoming_msg_queue_size=10, outgoing_msg_queue_size=10, socket_buf_size=8192):
        super().__init__(max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, socket_buf_size)
        self.stream_reader = stream_reader
        self.stream_writer = stream_writer
        self.closing_socket = False

    async def _read_data(self, size):
        try:
            data = await self.stream_reader.read(size)
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

    async def _write_data(self, data):
        log.debug(f'Sending data: {data}')
        self.stream_writer.write(data)
        await self.stream_writer.drain()

    async def close(self):
        self.closing_socket = True
        self.stream_writer.close()
        await self.stream_writer.wait_closed()
    
class AsyncTcpConnector:
    def __init__(self, engine: ProtocolEngine, dispatcher, func_registers=None):
        self.engine = engine
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.initiator = True
        self.unix_socket_path = None
        
    async def connect(self, host, port):
        if port is None:
            self.unix_socket_path = host
        if self.unix_socket_path:
            reader, writer = await asyncio.open_unix_connection(path=self.unix_socket_path)
        else:
            reader, writer = await asyncio.open_connection(host, port)
        transport = AsyncTcpTransport(reader, writer)
        await transport.start()
        return AsyncMsgChannel(transport, initiator=self.initiator, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)


class AsyncUnixSocketConnector(AsyncTcpConnector):
    def __init__(self, engine_choices, dispatcher, func_registers=None, handshake_cls=None):
        super().__init__(engine_choices, dispatcher, func_registers)

    async def connect(self, path):
        return await super().connect(path, None)


class AsyncTcpListener:
    def __init__(self, engine: ProtocolEngine, dispatcher, func_registers=None):
        self.engine = engine
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.initiator = False
        self.tcp_server = None
        self.address = None
        self.channel_tasks = dict()  # remote_address -> task
        self.connected_channels = dict()  # remote_address -> channel
        self.listening_task = None
        self.time_to_stop = Event()
        self.unix_socket_path = None
        self.replace_unix_socket_if_in_use = False
        self.server_socket = None
        self.async_server = None

    async def _detect_unix_socket_in_use(self, socket_path):
        try:
            reader, writer = await asyncio.open_unix_connection(path=self.unix_socket_path)
            writer.close()
            await writer.wait_closed()
            return True
        except Exception:
            return False

    async def run_server(self, bind_address, port, block=True):
        self.listening_task = asyncio.create_task(self._run_server(bind_address, port))
        if block:
            await self.block()

    async def _run_server(self, bind_address, port):
        if port is None:
            self.address = bind_address
            self.unix_socket_path = bind_address
        else:
            self.address = f'{bind_address}:{port}'
        if sys.implementation.name == 'micropython':
            if self.unix_socket_path:
                raise RuntimeError('Unix sockets not supported on micropython')
            self.async_server = await asyncio.start_server(self._async_client_connected, bind_address, port)
            await self.async_server.wait_closed()
        else:
            if self.unix_socket_path:
                self.async_server = await asyncio.start_unix_server(self._async_client_connected, path=self.unix_socket_path)
            else:
                self.async_server = await asyncio.start_server(self._async_client_connected, bind_address, port, reuse_address=True)
            log.info(f'Listing on {self.address}')
            await self.async_server.wait_closed()

    async def _async_client_connected(self, async_reader: asyncio.StreamReader, async_writer: asyncio.StreamWriter):
        # FIXME: work out what the peername format is in micropython
        remote_address = repr(async_writer.get_extra_info('peername'))
        log.info(f'REMOTE ADDRESS: {remote_address}')
        transport = AsyncTcpTransport(async_reader, async_writer)
        channel_task = asyncio.create_task(self._start_channel(transport, remote_address))
        self.channel_tasks[remote_address] = channel_task

    async def shutdown_server(self):
        log.info('Starting Server Shutdown')
        self.listening_task.cancel()
        try:
            await self.listening_task
        except asyncio.CancelledError:
            pass
        for channel in self.connected_channels.values():
            assert isinstance(channel, AsyncMsgChannel)
            await channel.shutdown_channel()
        # cleanup unix socket if we are not replacing by default
        if self.unix_socket_path and not self.replace_unix_socket_if_in_use:
            os.remove(self.unix_socket_path)
        log.info('Server Shutdown Complete')

    async def _start_channel(self, transport: AsyncTcpTransport, remote_address):
        await transport.start()
        channel = AsyncMsgChannel(transport, initiator=False, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)
        self.connected_channels[remote_address] = channel
        await channel.start_channel()

    def _signal_handler(self, signum, frame):
        signame = signal.Signals(signum).name
        log.info(f'Signal handler called with signal {signame} ({signum})')
        self.listening_task.cancel()

    async def block(self, signals=None):
        signal_handler_installer = None
        if not running_micropython:
            signal_handler_installer = SignalHandlerInstaller(signals)
            signal_handler_installer.install(self._signal_handler)
        try:
            try:
                await self.listening_task
            except asyncio.CancelledError:
                pass
        finally:
            if signal_handler_installer:
                signal_handler_installer.remove()
        await self.shutdown_server()


class AsyncUnixSocketListener(AsyncTcpListener):
    def __init__(self, engine_choices, dispatcher, func_registers=None):
        super().__init__(engine_choices, dispatcher, func_registers)

    async def run_server(self, path, replace_if_in_use=False, block=True):
        self.replace_unix_socket_if_in_use = replace_if_in_use
        await super().run_server(path, None, block)


class StreamTransport(AsyncTransportBase):
    def __init__(self, incoming_stream, outgoing_stream, max_msg_size=10*1024*1024, max_bc_length=10, incoming_msg_queue_size=10, outgoing_msg_queue_size=10, read_buf_size=8192):
        super().__init__(max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, read_buf_size)
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
        assert isinstance(engine, ProtocolEngine)
        self.engine = engine
        self.dispatcher = dispatcher
        self.func_registers = func_registers

    @abstractmethod
    async def get_channel(self, child_stdin=None, child_stdout=None):
        raise NotImplementedError()    


class ParentSubprocessRunner(SubprocessRunnerBase):
    async def get_channel(self, child_stdin, child_stdout):
        transport = StreamTransport(outgoing_stream=child_stdin, incoming_stream=child_stdout)
        return AsyncMsgChannel(transport, initiator=True, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)


class ChildSubprocessRunner(SubprocessRunnerBase):
    def get_channel(self):
        transport = StreamTransport(sys.stdin.buffer, sys.stdout.buffer)
        return AsyncMsgChannel(transport, initiator=False, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)
