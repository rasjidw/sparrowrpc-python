
import logging
import signal

if 'threaded' in __name__: #= remove
    from threading import Thread #= threaded <
    from websockets.sync import client  #= threaded <
    from websockets.sync import server  #= threaded <
else: #= remove
    import asyncio #= async <
    from websockets.asyncio import client  #= async <
    from websockets.asyncio import server  #= async <

import websockets.exceptions

from ...engine import ProtocolEngine
from ...lib import SignalHandlerInstaller
from ..._template_ import _Template_MsgChannel
from ..transports import _Template_TransportBase
from ...bases import DEFAULT_SERIALISER_SIG, DEFAULT_ENCODER_TAG

log = logging.getLogger(__name__)


class _Template_WebsocketTransport(_Template_TransportBase):
    def __init__(self, websocket, max_msg_size=10*1024*1024, max_bc_length=10, incoming_msg_queue_size=10,
                 outgoing_msg_queue_size=10, socket_buf_size=8192):
        super().__init__(max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, socket_buf_size)
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
    def __init__(self, engine, dispatcher, default_serialiser_sig: str=DEFAULT_SERIALISER_SIG,
                 default_encoder_tag: str=DEFAULT_ENCODER_TAG, func_registers=None, handshake_cls=None):
        self.engine = engine
        self.dispatcher = dispatcher
        self.default_serialiser_sig = default_serialiser_sig
        self.default_encoder_tag = default_encoder_tag
        self.func_registers = func_registers
        self.initiator = True

    async def connect(self, ws_uri):
        websocket = await client.connect(ws_uri)
        transport = _Template_WebsocketTransport(websocket)  # FIX_ME: Allow options to be set / passed in??
        return _Template_MsgChannel(transport, default_serialiser_sig=self.default_serialiser_sig,
                                    default_encoder_tag=self.default_encoder_tag, initiator=self.initiator,
                                    engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)


class _Template_WebsocketListener:
    def __init__(self, engine: ProtocolEngine, dispatcher, default_serialiser_sig: str=DEFAULT_SERIALISER_SIG,
                 default_encoder_tag: str=DEFAULT_ENCODER_TAG, func_registers=None):
        self.engine = engine
        self.dispatcher = dispatcher
        self.default_serialiser_sig = default_serialiser_sig
        self.default_encoder_tag = default_encoder_tag
        self.func_registers = func_registers
        self.initiator = False
        self.websocket_server = None
        self.listening_thread = None  #= threaded
        self.listening_task = None  #= async
        self.connected_channels = dict()  # remote_address -> channel
        self.time_to_stop = False

    async def run_server(self, bind_address, port, block=True):
        #= threaded start
        self.listening_thread = Thread(target=self._run_server, args=(bind_address, port))
        self.listening_thread.start()
        #= threaded end
        #= async start
        self.listening_task = asyncio.create_task(self._run_server(bind_address, port))
        #= async end
        if block:
            await self.block()

    async def _run_server(self, bind_address, port):
        async with server.serve(self._websocket_handler, bind_address, port) as self.websocket_server: 
            log.info(f'Websocket Listing on {bind_address}:{port}')
            #= threaded start
            # await is removed during generation from template  #= remove
            await self.websocket_server.serve_forever()
            #= threaded end
            #= async start
            try:
                await self.websocket_server.serve_forever()
            except asyncio.CancelledError:
                pass  # this seems to be raised on server close. Don't re-raise it so we can shut down cleanly.
            #= async end

    def _signal_handler(self, signum, frame):
        signame = signal.Signals(signum).name
        log.info(f'Stop listening signal handler called with signal {signame} ({signum})')
        self.stop_listening()

    def stop_listening(self):
        self.websocket_server.shutdown()  #= threaded
        self.websocket_server.close(close_connections=False)  #= async

    async def block(self, signals=None):
        signal_handler_installer = SignalHandlerInstaller(signals)
        log.debug('Installing signal handlers')
        signal_handler_installer.install(self._signal_handler)
        try:
            #= threaded start
            while True:
                # timeout required for windows or we never get the SIGINT signal
                self.listening_thread.join(timeout=0.5)
                if not self.listening_thread.is_alive():
                    break
            #= threaded end
            #= async start
            await self.listening_task
            #= async end
        finally:
            log.debug('Removing signal handlers')
            signal_handler_installer.remove()
        await self.shutdown_server()

    async def shutdown_server(self):
        log.info('Starting Server Shutdown')
        self.stop_listening()
        self.listening_thread.join() #= threaded
        await self.listening_task #= async

        self.time_to_stop = True
        for channel in self.connected_channels.values():
            assert isinstance(channel, _Template_MsgChannel)
            await channel.shutdown_channel()
        log.info('Server Shutdown Complete')

    async def _websocket_handler(self, client_websocket):
        websocket_path = client_websocket.request.path
        requested_engine_sig = websocket_path.strip('/')
        remote_address = 'FIXME'
        log.info(f'Accepted connection request from {remote_address} with path {websocket_path}')
        transport = _Template_WebsocketTransport(client_websocket)
        channel = _Template_MsgChannel(transport, initiator=False, engine=self.engine, dispatcher=self.dispatcher,
                                       func_registers=self.func_registers)
        self.connected_channels[remote_address] = channel
        await channel.start_channel()
        await channel.wait_for_remote_close()
