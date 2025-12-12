
import logging
import sys

from pyscript import WebSocket

import asyncio
try:
    from asyncio import Queue, QueueEmpty
except (AttributeError, ImportError):
    from uasync.queues import Queue, QueueEmpty # type: ignore

from .asyncio import AsyncMsgChannel
from .asyncio.transports import AsyncTransportBase
from .lib import portable_format_exc

log = logging.getLogger(__name__)


class PyscriptWebsocketTransport(AsyncTransportBase):
    def __init__(self, max_msg_size=10*1024*1024, max_bc_length=10, incoming_msg_queue_size=10, outgoing_msg_queue_size=10, socket_buf_size=8192):
        AsyncTransportBase.__init__(self, max_msg_size, max_bc_length, incoming_msg_queue_size, outgoing_msg_queue_size, socket_buf_size)
        self.ws_url = None
        self.js_ws = None
        self.raw_incoming = Queue()   # queue of raw incoming messages, terminated by None (signals close)
        self.open_complete = Queue()  # None if open successful, or an exception otherwise

    async def connect(self, ws_url):
        self.ws_url = ws_url
        if sys.implementation.name == 'micropython':
            onmessage = self.mpy_js_onmessage
            log.info('In micropython')
        else:
            onmessage = self.js_onmessage
        self.js_ws = WebSocket(url=ws_url, onopen=self.js_onopen, onmessage=onmessage, onclose=self.js_onclose, onerror=self.js_onerror)
        open_result = await self.open_complete.get()
        if open_result:  # an error
            log.error(f'Got an open_result error of {open_result}')
            raise open_result
        else:
            log.debug(f'connect to {self.ws_url} successful')

    async def js_onopen(self, event):
        log.debug(f'Got onopen event: {event!r}')
        await self.open_complete.put(None)

    async def js_onerror(self, event):
        log.debug(f'Got Javascript WS error: {event}')
        # FIXME: Check what state we are in
        # FIXME: better error
        e = RuntimeError('Error connecting')
        await self.open_complete.put(e)

    def mpy_js_onmessage(self, event):
        log.info(f'mpy got incoming data: {event.data!s}')
        loop = asyncio.get_event_loop()
        loop.create_task(self.js_onmessage(event))

    async def js_onmessage(self, event):
        log.info(f'Got incoming data: {event.data!s}')
        try:
            js_data = event.data
            if isinstance(js_data, str):
                log.error(f'Got non-binary data message - it is being dropped!')
            else:
                if sys.implementation.name == 'micropython':
                    incoming_data = bytes.fromhex(js_data.hex())
                    log.debug(f'Got incoming_data: {incoming_data}')
                else:
                    incoming_data = js_data.tobytes()
                await self.raw_incoming.put(incoming_data)
        except Exception as e:
            log.error(f'Error in js_onmessage: {e!s}')

    async def js_onclose(self, event):
        log.debug(f'Got onclose event: {event!r}')
        await self.raw_incoming.put(None)

    async def _read_data(self, size):
        data = await self.raw_incoming.get()
        if data is None:
            return ''
        if not data:
            log.warning(f'Got empty incoming message - it is being dropped')
        log.debug(f'Got incoming data: {data}')
        return data

    async def _write_data(self, data):
        log.info(f'Sending data: {data}')
        try:
            self.js_ws.send(data)
            log.info(f'Send complete: {data}')
        except Exception as e:
            log.error(f'_write_data error: {portable_format_exc(e)}')

    async def close(self):
        await self.js_ws.close()

    
class PyscriptWebsocketConnector:
    def __init__(self, engine, dispatcher, func_registers=None):
        self.engine = engine
        self.dispatcher = dispatcher
        self.func_registers = func_registers
        self.initiator = True

    async def connect(self, ws_url):
        transport = PyscriptWebsocketTransport()  # FIX_ME: Allow options to be set / passed in??
        await transport.connect(ws_url)
        return AsyncMsgChannel(transport, initiator=self.initiator, engine=self.engine, dispatcher=self.dispatcher, func_registers=self.func_registers)
