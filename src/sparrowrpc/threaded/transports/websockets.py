# -----------------------------------------------------------------------
# WARNING: This file is auto-generated and should not be edited manually!
#
# It is generated using 'generate_from_template_code.py' using the
# _template_ directory as the source.
# -----------------------------------------------------------------------


import logging
import signal

from threading import Thread
from websockets.sync import client
from websockets.sync import server

import websockets.exceptions

from ...bases import ProtocolEngineBase
from ...lib import SignalHandlerInstaller
from ...threaded import ThreadedMsgChannel
from ..transports import ThreadedTransportBase


log = logging.getLogger(__name__)


class ThreadedWebsocketTransport(ThreadedTransportBase):
    def __init__(self, websocket, max_msg_size=10*1024*1024, max_bc_length=10, incoming_msg_queue_size=10, outgoing_msg_queue_size=10, socket_buf_size=8192):
        super().__init__(max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, socket_buf_size)
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

    
class ThreadedWebsocketConnector:
    def __init__(self, engine, dispatcher, func_registers=None, handshake_cls=None):
        self.engine = engine
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.initiator = True

    def connect(self, ws_uri):
        websocket = client.connect(ws_uri)
        transport = ThreadedWebsocketTransport(websocket)  # FIX_ME: Allow options to be set / passed in??
        return ThreadedMsgChannel(transport, initiator=self.initiator, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)


class ThreadedWebsocketListener:
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
        self.listening_thread = None
        self.connected_channels = dict()  # remote_address -> channel
        self.time_to_stop = False

    def run_server(self, bind_address, port, block=True):
        self.listening_thread = Thread(target=self._run_server, args=(bind_address, port))
        self.listening_thread.start()
        if block:
            self.block()

    def _run_server(self, bind_address, port):
        with server.serve(self._websocket_handler, bind_address, port) as self.websocket_server: 
            log.info(f'Websocket Listing on {bind_address}:{port}')
            self.websocket_server.serve_forever()

    def _signal_handler(self, signum, frame):
        signame = signal.Signals(signum).name
        log.info(f'Stop listening signal handler called with signal {signame} ({signum})')
        self.stop_listening()

    def stop_listening(self):
        self.websocket_server.shutdown()

    def block(self, signals=None):
        signal_handler_installer = SignalHandlerInstaller(signals)
        log.debug('Installing signal handlers')
        signal_handler_installer.install(self._signal_handler)
        try:
            while True:
                # timeout required for windows or we never get the SIGINT signal
                self.listening_thread.join(timeout=0.5)
                if not self.listening_thread.is_alive():
                    break
        finally:
            log.debug('Removing signal handlers')
            signal_handler_installer.remove()
        self.shutdown_server()

    def shutdown_server(self):
        log.info('Starting Server Shutdown')
        self.stop_listening()
        self.listening_thread.join()

        self.time_to_stop = True
        for channel in self.connected_channels.values():
            assert isinstance(channel, ThreadedMsgChannel)
            channel.shutdown_channel()
        log.info('Server Shutdown Complete')

    def _websocket_handler(self, client_websocket):
        websocket_path = client_websocket.request.path
        requested_engine_sig = websocket_path.strip('/')
        remote_address = 'FIXME'
        engine = self.engine_lookup.get(requested_engine_sig)
        if engine:
            log.info(f'Accepted connection request from {remote_address} with path {websocket_path}')
            transport = ThreadedWebsocketTransport(client_websocket)
            channel = ThreadedMsgChannel(transport, initiator=False, engine=engine, dispatcher=self.dispatcher, func_registers=self.func_registers)
            self.connected_channels[remote_address] = channel
            channel.start_channel()
            channel.wait_for_remote_close()
        else:
            log.info(f'REJECTED connection request from {remote_address} with path {websocket_path}')
