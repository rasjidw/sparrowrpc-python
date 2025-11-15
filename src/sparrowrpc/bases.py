from __future__ import annotations

import logging

from binarychain import BinaryChain

from .engine import ProtocolEngine
from .serialisers import JsonSerialiser
from .encoders.v050 import V050EncoderDecoder
from .messages import Acknowledge, ExceptionResponse, Notification, Request, Response, ExceptionCategory, ExceptionInfo, CallBase, RequestType, ResponseType
from .registers import FuncInfo, FunctionRegister, default_func_register, global_channel_register


log = logging.getLogger(__name__)


DEFAULT_SERIALISER_SIG = JsonSerialiser.sig
DEFAULT_ENCODER_TAG = V050EncoderDecoder.TAG


class MsgChannelBase:
    def __init__(self, initiator: bool, engine: ProtocolEngine, default_serialiser_sig: str, default_encoder_tag: str,
                 channel_tag='', func_registers=None, channel_register=None):
        self.initiator = initiator
        self.engine = engine

        if default_serialiser_sig not in self.engine.serialisers.keys():
            raise ValueError(f'Serialiser sig of {default_serialiser_sig} not found in engine')
        self.default_serialiser_sig = default_serialiser_sig

        if default_encoder_tag not in self.engine.encoders.keys():
            raise ValueError(f'Encoder tag of {default_encoder_tag} not found in engine')

        self.default_encoder_tag = default_encoder_tag
        self.tag = channel_tag
        self._channel_register = channel_register if channel_register else global_channel_register

        self.system_register = self.engine.get_system_register()
        self.registers = [self.system_register]

        if func_registers:
            if isinstance(func_registers, list):
                for item in func_registers:
                    if isinstance(item, FunctionRegister):
                        self.registers.append(func_registers)
                    else:
                        raise TypeError()
            elif isinstance(func_registers, FunctionRegister):
                self.registers.append(func_registers)
            else:
                raise TypeError()
        else:
            self.registers.append(default_func_register)

        self._message_id = 1
        self._message_event_callbacks = dict()  # message_id -> callable

    def add_register(self, func_register):
        assert isinstance(func_register, FunctionRegister)
        self.registers.append(func_register)

    def _lookup_func_register(self, target, namespace):
        for func_register in self.registers:
            assert isinstance(func_register, FunctionRegister)
            func_info = func_register.get_method_info(target, namespace)
            if func_info:
                return func_info
        return None

    def _get_add_id_and_reg_cb(self, message):
        add_id = self.engine.always_send_ids
        register_event_callback = False
        if isinstance(message, Request):
            add_id = True
            register_event_callback = True
        if isinstance(message, Acknowledge):
            # can't add ids to outgoing acknowledge
            add_id = False
        if isinstance(message, Notification):
            # can't add ids to outgoing notifications
            add_id = False
        if isinstance(message, Response):
            if message.acknowledge:
                add_id = True
                register_event_callback = True
        return add_id, register_event_callback

    def _reg_callback(self, message_id, message_event_callback):
            if message_id is None:
                raise RuntimeError('invalid state - message id should be set')
            if not message_event_callback:
                raise ValueError('message_event_callback required')
            self._message_event_callbacks[message_id] = message_event_callback

    def _get_func_info_and_ack_err_msg(self, message: CallBase):
        ack_err_msg = None
        func_info = self._lookup_func_register(message.target, message.namespace)
        if func_info:
            assert isinstance(func_info, FuncInfo)
            if isinstance(message, Request) and message.acknowledge:
                if message.request_type == RequestType.SILENT:
                    log.warning(f'Silent Request {message} flagged with Acknowledge')
                else:
                    if message.message_id is None:
                        log.error(f'Incoming request without an id')
                    else:
                        ack_err_msg = Acknowledge(request_id=message.message_id)
        else:
            if isinstance(message, Request):
                exc_info = ExceptionInfo(ExceptionCategory.CALLER, exc_type='TargetNotFound', msg=f'target {message.target} not found')
                ack_err_msg = ExceptionResponse(message.message_id, exc_info=exc_info)
        return func_info, ack_err_msg

    def _parse_and_allocate_bin_chain(self, bin_chain: BinaryChain):
        dispatch = False
        incoming_callback = None
        log.debug(f'Got incoming binary chain: {repr(bin_chain)}')
        message = self.engine.parse_incoming_message(bin_chain)
        log.debug(f'Got incoming message {message}')

        if (isinstance(message, Request) or isinstance(message, Notification)) and not message.callback_request_id:
            dispatch = True
        else:
            request_completed = False
            if (isinstance(message, Request) or isinstance(message, Notification)) and message.callback_request_id:
                request_id = message.callback_request_id
            else:
                assert isinstance(message, (Response, ExceptionResponse, Acknowledge))
                request_id = message.request_id
            if isinstance(message, Response):
                if message.response_type == ResponseType.NORMAL:
                    request_completed = True
                elif message.response_type == ResponseType.MULTIPART:
                    request_completed = message.final
                else:
                    raise RuntimeError(f'Invalid response type of {message.response_type!r}')
            if isinstance(message, ExceptionResponse):
                request_completed = True
            try:
                if request_completed:
                    incoming_callback = self._message_event_callbacks.pop(request_id)
                else:
                    incoming_callback = self._message_event_callbacks[request_id]
            except KeyError:
                log.warning(f'Incoming message {message} with invalid request id')
        return message, dispatch, incoming_callback
