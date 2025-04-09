
import logging
from threading import Thread

import websockets.sync.client
import websockets.sync.server

from lucido.threaded import TransportBase, ProtocolEngine, MsgChannel


log = logging.getLogger(__name__)


class WebsocketTransport(TransportBase):
    def __init__(self, engine, websocket, max_msg_size=10*1024*1024, incoming_msg_queue_size=10, outgoing_msg_queue_size=10, socket_buf_size=8192):
        TransportBase.__init__(self, engine, max_msg_size, incoming_msg_queue_size, outgoing_msg_queue_size, socket_buf_size)
        self.websocket = websocket

    def _read_data(self, size):
        try:
            return self.websocket.recv()
        except websockets.exceptions.ConnectionClosed:
            return ''

    def _write_data(self, data):
        log.debug(f'Sending data: {data}')
        self.websocket.send(data)

    def close(self):
        self.websocket.close()

    
class WebsocketConnector:
    def __init__(self, engine, dispatcher, func_registers=None, handshake_cls=None):
        self.engine = engine
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.initiator = True

    def connect(self, ws_uri):
        websocket = websockets.sync.client.connect(ws_uri)
        transport = WebsocketTransport(self.engine, websocket)  # FIX_ME: Allow options to be set / passed in??
        return MsgChannel(transport, initiator=self.initiator, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)


class WebsocketListener:
    def __init__(self, engine_choices, dispatcher, func_registers=None):
        if isinstance(engine_choices, ProtocolEngine):
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

    def run_server(self, bind_address, port):
        with websockets.sync.server.serve(self._websocket_handler, bind_address, port) as self.websocket_server: 
            log.info(f'Listing on {bind_address}:{port}')
            try:
                self.websocket_server.serve_forever()
            except KeyboardInterrupt:
                self.shutdown_server()

    def shutdown_server(self):
        log.info('Starting Server Shutdown')
        self.time_to_stop = True
        for channel in self.connected_channels.values():
            assert isinstance(channel, MsgChannel)
            channel.shutdown_channel()
        self.websocket_server.shutdown()
        log.info('Server Shutdown Complete')

    def _websocket_handler(self, client_websocket):
        websocket_path = client_websocket.request.path
        requested_engine_sig = websocket_path.strip('/')
        remote_address = 'FIXME'
        engine = self.engine_lookup.get(requested_engine_sig)
        if engine:
            log.info(f'Accepted connection request from {remote_address} with path {websocket_path}')
            transport = WebsocketTransport(engine, client_websocket)
            channel = MsgChannel(transport, initiator=False, engine=engine, dispatcher=self.dispatcher, func_registers=self.func_registers)
            self.connected_channels[remote_address] = channel
            channel.start_channel()
            channel.wait_for_remote_close()
        else:
            log.info(f'REJECTED connection request from {remote_address} with path {websocket_path}')
