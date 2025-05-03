
import logging
from threading import Thread

if 'threaded' in __name__: #= remove
    from websockets.sync import client  #= threaded <
    from websockets.sync import server  #= threaded <
else: #= remove
    from websockets.asyncio import client  #= async <
    from websockets.asyncio import server  #= async <

import websockets.exceptions

from ...core import ProtocolEngineBase
from ..._template_ import _Template_MsgChannel
from ..transports import _Template_TransportBase


log = logging.getLogger(__name__)


class _Template_WebsocketTransport(_Template_TransportBase):
    def __init__(self, websocket, max_msg_size=10*1024*1024, max_bc_length=10, incoming_msg_queue_size=10, outgoing_msg_queue_size=10, socket_buf_size=8192):
        _Template_TransportBase.__init__(self, max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, socket_buf_size)
        self.websocket = websocket

    async def _read_data(self, size):
        try:
            return await self.websocket.recv()
        except websockets.exceptions.ConnectionClosed:
            return ''

    async def _write_data(self, data):
        log.debug(f'Sending data: {data}')
        await self.websocket.send(data)

    async def close(self):
        await self.websocket.close()

    
class _Template_WebsocketConnector:
    def __init__(self, engine, dispatcher, func_registers=None, handshake_cls=None):
        self.engine = engine
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.initiator = True

    async def connect(self, ws_uri):
        websocket = await client.connect(ws_uri)
        transport = _Template_WebsocketTransport(websocket)  # FIX_ME: Allow options to be set / passed in??
        return _Template_MsgChannel(transport, initiator=self.initiator, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)


class _Template_WebsocketListener:
    def __init__(self, engine_choices, dispatcher, func_registers=None):
        if isinstance(engine_choices, ProtocolEngineBase):
            self.engine_choices = [engine_choices]
        else:
            self.engine_choices = engine_choices
        self.engine_lookup = {engine.get_engine_signature(): engine for engine in self.engine_choices}
        if not self.engine_choices:
            raise ValueError('At least one engine choice must be passed in')
        if len(self.engine_choices) != len(self.engine_lookup):
            raise ValueError('Duplicate engine signatures')
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.initiator = False
        self.websocket_server = None
        self.connected_channels = dict()  # remote_address -> channel
        self.time_to_stop = False

    async def run_server(self, bind_address, port):
        async with server.serve(self._websocket_handler, bind_address, port) as self.websocket_server: 
            log.info(f'Listing on {bind_address}:{port}')
            try:
                await self.websocket_server.serve_forever()
            except KeyboardInterrupt:
                await self.shutdown_server()

    async def shutdown_server(self):
        log.info('Starting Server Shutdown')
        self.time_to_stop = True
        for channel in self.connected_channels.values():
            assert isinstance(channel, _Template_MsgChannel)
            await channel.shutdown_channel()
        await self.websocket_server.shutdown()
        log.info('Server Shutdown Complete')

    async def _websocket_handler(self, client_websocket):
        websocket_path = client_websocket.request.path
        requested_engine_sig = websocket_path.strip('/')
        remote_address = 'FIXME'
        engine = self.engine_lookup.get(requested_engine_sig)
        if engine:
            log.info(f'Accepted connection request from {remote_address} with path {websocket_path}')
            transport = _Template_WebsocketTransport(client_websocket)
            channel = _Template_MsgChannel(transport, initiator=False, engine=engine, dispatcher=self.dispatcher, func_registers=self.func_registers)
            self.connected_channels[remote_address] = channel
            await channel.start_channel()
            await channel.wait_for_remote_close()
        else:
            log.info(f'REJECTED connection request from {remote_address} with path {websocket_path}')
