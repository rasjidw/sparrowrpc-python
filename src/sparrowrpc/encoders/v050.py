from __future__ import annotations

import logging
from typing import Dict, Any, Union, Tuple, TYPE_CHECKING

from ..messages import (Acknowledge, Notification, Request, Response, ExceptionCategory, ExceptionInfo, CallBase, MessageBase, ResponseBase,
                        RequestCallbackInfo, RequestType, ResponseType, ExceptionResponse, IterableCallbackInfo, FinalType)

from .base import EncoderDecoderBase, make_out_except_data
from ..exceptions import ProtocolError

if TYPE_CHECKING:
    from ..engine import ProtocolEngine


log = logging.getLogger(__name__)


class V050EncoderDecoder(EncoderDecoderBase):
    TAG = '.5'
    max_bc_length = 3   # envelope, params or result, callback params (prefix not counted)
    envelope_marker_map = {'r': (Request, 'target', set('qs')),
                           'n': (Notification, 'target', None),
                           'p': (Response, 'request_id', set('m')),
                           'x': (ExceptionResponse, 'request_id', None),
                           'a': (Acknowledge, 'request_id', None)
                           }
    envelope_unique = set(envelope_marker_map.keys())
    envelope_sup = {'i': 'message_id',  # always allowed, but may be ignored
                    's': 'namespace',
                    'd': 'node',
                    'c': 'callback_request_id',
                    'v': 'protocol_version',  # required
                    'l': 'payload_serialisation_code',   # if missing, same as envelope serialisation code
                    }
    flags = {'a': 'acknowledge'}
    payload_info_key = 'f'  # if missing, default payload for message type
                            # FIXME: Not used yet. Use for binary in requests?

    def __init__(self, protocol_engine: ProtocolEngine):
        EncoderDecoderBase.__init__(self, protocol_engine)
        self._env_make_map = {Request: self._make_request_env,
                              Notification: self._make_notification_env,
                              Response: self._make_response_env,
                              ExceptionResponse: self._make_exception_env,
                              Acknowledge: self._make_ack_env,
                              }
        
        self._payload_make_map = {Request: self._make_request_payload,
                                  Notification: self._make_notification_payload,
                                  Response: self._make_response_payload,
                                  ExceptionResponse: self._make_exception_payload,
                                  Acknowledge: self._make_ack_payload,
                                  }
    
    def _get_payload_serialiser(self, message: MessageBase):
        # get the serialisers
        psc = message.envelope_serialisation_code
        if isinstance(message, (CallBase, ResponseBase)):
            if message.payload_serialisation_code: 
                psc = message.payload_serialisation_code

        if not psc:
            raise ValueError('no serialisation code set')

        try:
            payload_serialiser = self.protocol_engine.serialisers[psc]
        except KeyError:
            raise ValueError(f'Serialiser "{psc}" not found')

        return payload_serialiser
        
    def serialise_payload_data(self, message: MessageBase):
        try:
            # noinspection PyTypeChecker
            make_method = self._payload_make_map[type(message)]
            return make_method(message)
        except KeyError:
            raise TypeError(f'Invalid message type {type(message)}')
    
    def encode_envelope(self, message: MessageBase):
        try:
            # noinspection PyTypeChecker
            make_method = self._env_make_map[type(message)]
            return make_method(message)
        except KeyError:
            raise TypeError(f'Invalid message type {type(message)}')

    def _make_request_env(self, message: Request):
        if message.message_id is None:
            raise ValueError('message id is required on Outgoing Requests')
        marker_chrs = ['r']
        if message.request_type == RequestType.SILENT and message.acknowledge:
            raise ValueError('acknowledge can not use set on SILENT requests.')
        if message.acknowledge:
            marker_chrs.append('a')
        if message.request_type != RequestType.NORMAL:
            marker_chrs.append(str(message.request_type))
        envelope_data = {''.join(marker_chrs): message.target, 'i': message.message_id, 'v': message.protocol_version}
        if message.callback_request_id:
            envelope_data['c'] = message.callback_request_id
        if message.node:
            envelope_data['d'] = message.node
        if message.namespace:
            envelope_data['s'] = message.namespace
        if message.payload_serialisation_code != message.envelope_serialisation_code:
            envelope_data['l'] = message.payload_serialisation_code
        return envelope_data

    def _make_request_payload(self, message: Request):
        payload_parts = list()
        payload_serialiser = self._get_payload_serialiser(message)

        if message.params or message.callback_params:
            param_part = payload_serialiser.serialise(message.params) if message.params else b''
            payload_parts.append(param_part)

            if message.callback_params:
                cb_dict = {}
                icb_dict = {}
                for param_name, cb_info in message.callback_params.items():
                    if isinstance(cb_info, RequestCallbackInfo):
                        cb_dict[param_name] = dict(p=cb_info.param_details)
                    elif isinstance(cb_info, IterableCallbackInfo):
                        icb_dict[param_name] = dict(r=cb_info.return_details)
                    else:
                        raise TypeError(f'Invalid callback param type {type(cb_info)}')
                special_params = {'#cb': cb_dict, '#icb': icb_dict}
                special_params_part = payload_serialiser.serialise(special_params)
                payload_parts.append(special_params_part)
        return payload_parts
        
    def _make_notification_env(self, message: Notification):
        envelope_data: Dict[str, Any] = {'n': message.target, 'v': message.protocol_version}
        if message.node:
            envelope_data['d'] = message.node
        if message.namespace:
            envelope_data['s'] = message.namespace
        if message.callback_request_id:
            envelope_data['c'] = message.callback_request_id
        if message.payload_serialisation_code != message.envelope_serialisation_code:
            envelope_data['l'] = message.payload_serialisation_code
        return envelope_data
    
    def _make_notification_payload(self, message: Notification):
        payload_parts = list()
        payload_serialiser = self._get_payload_serialiser(message)

        if message.params:
            param_part = payload_serialiser.serialise(message.params)
            payload_parts.append(param_part)
        return payload_parts

    def _make_response_env(self, message: Response):
        marker = 'p'
        marker += message.response_type
        if message.final:
            marker += message.final
        if message.acknowledge:
            if not message.message_id:
                raise ValueError('A message id must be given when requesting an ack')
            marker += 'a'
        envelope_data: Dict[str, Any] = {marker: message.request_id, 'v': message.protocol_version}
        if message.message_id is not None:
            envelope_data['i'] = message.message_id
        if message.payload_serialisation_code != message.envelope_serialisation_code:
            envelope_data['l'] = message.payload_serialisation_code
        return envelope_data
    
    def _make_response_payload(self, message: Response):
        payload_parts = list()
        payload_serialiser = self._get_payload_serialiser(message)

        if message.result is not None:  # FIXME? Do we need to allow returning of None, and use Void instead?
            payload_parts.append(payload_serialiser.serialise(message.result))
        return payload_parts

    def _make_exception_env(self, message: ExceptionResponse):
        marker = 'x'
        if message.acknowledge:
            if not message.message_id:
                raise ValueError('A message id must be given when requesting an ack')
            marker += 'a'
        envelope_data: Dict[str, Any] = {marker: message.request_id, 'v': message.protocol_version}
        if message.message_id is not None:
            envelope_data['i'] = message.message_id
        if message.payload_serialisation_code != message.envelope_serialisation_code:
            envelope_data['l'] = message.payload_serialisation_code
        return envelope_data

    def _make_exception_payload(self, message: ExceptionResponse):
        except_data = make_out_except_data(message)
        payload_serialiser = self._get_payload_serialiser(message)
        return [payload_serialiser.serialise(except_data)]
    
    def _make_ack_env(self, message: Acknowledge):
        return {'a': message.request_id, 'v': message.protocol_version}
    
    def _make_ack_payload(self, message: Acknowledge):
        return []

    def parse_payload_data(self, message: Union[CallBase, ResponseBase], raw_payload_data: list):
        assert isinstance(message, (CallBase, ResponseBase))
        payload_serialisation_code = message.payload_serialisation_code if message.payload_serialisation_code else message.envelope_serialisation_code
        if not payload_serialisation_code:
            raise ProtocolError('No envelope serialisation code set')
        
        try:
            serialiser = self.protocol_engine.serialisers[payload_serialisation_code]
        except KeyError:
            raise ProtocolError(f'Serialiser for {payload_serialisation_code} not found')

        if isinstance(message, Request):
            if not raw_payload_data:
                return message

            # normal params
            if raw_payload_data[0]:
                message.params = serialiser.deserialise(raw_payload_data[0])
            if len(raw_payload_data) > 1:
                special_params_dict = serialiser.deserialise(raw_payload_data[1])  # cb_param -> target_name
                cb_params = dict()
                raw_std_cb_params = special_params_dict.get('#cb', dict())
                for (param_name, cb_info) in raw_std_cb_params.items():   # FIXME: currently ignoring info
                    cb_params[param_name] = {'#cb': {'callback_request_id': message.message_id, 'cb_info': cb_info}}
                raw_iter_cb_params = special_params_dict.get('#icb', dict())
                for (param_name, cb_info) in raw_iter_cb_params.items():
                    cb_params[param_name] = {'#icb': {'callback_request_id': message.message_id, 'cb_info': cb_info}}
                message.callback_params = cb_params
            if len(raw_payload_data) > 2:
                raise ProtocolError('Extra payload data found')
            return message
        
        if isinstance(message, Notification):            
            message.params = serialiser.deserialise(raw_payload_data[0])
            return message
        
        if isinstance(message, Response):
            if raw_payload_data:
                message.result = serialiser.deserialise(raw_payload_data[0])
            return message
        
        if isinstance(message, ExceptionResponse):
            raw_except_data = serialiser.deserialise(raw_payload_data[0])
            message.exc_info = ExceptionInfo(ExceptionCategory(raw_except_data['cat']), raw_except_data['type'], raw_except_data['msg'])
            return message

        if isinstance(message, Acknowledge):
            return message
        
        raise RuntimeError('invalid message passed in')

    def decode_raw_envelope(self, envelope_data: dict, envelope_serialisation_code: str):
        if not isinstance(envelope_data, dict):
            raise ProtocolError('envelope must be a dict')
        key_type_to_flags = {key[0]: key[1:] for key in envelope_data.keys()}
        key_type_set = set(key_type_to_flags.keys())
        marker_list = list(key_type_set.intersection(self.envelope_unique))
        if len(marker_list) != 1:
            log.error(f'marker_list is {marker_list} but should be a single item')
            raise ProtocolError('single marker not found')
        marker = marker_list[0]
        cls, attr, valid_type_flags = self.envelope_marker_map[marker]
        flags = key_type_to_flags[marker]
        type_flags = set()
        if valid_type_flags:
            assert isinstance(valid_type_flags, set)
            type_flags = valid_type_flags.intersection(set(flags))
            if len(type_flags) > 1:
                raise ProtocolError('more than one type-flag found')
        actual_key = marker + flags
        kwargs = {attr: envelope_data.pop(actual_key)}
        for key, attr in self.envelope_sup.items():
            if key in envelope_data:
                kwargs[attr] = envelope_data.pop(key)
        if type_flags:
            attr, value = self._get_request_response_type(cls, type_flags)
            kwargs[attr] = value
        
        # set the envelope_serialisation_code as it is not in the envelope data itself 
        kwargs['envelope_serialisation_code'] = envelope_serialisation_code
        kwargs['protocol_version'] = self.TAG

        result = cls(**kwargs)
        for flag in flags:
            if flag == 'a':
                if hasattr(result, 'acknowledge'):
                    result.acknowledge = True
                else:
                    log.warning('Got unexpected acknowledge flag')  # FIXME: make these warning protocol errors?
            elif flag in ('f', 't'):
                if hasattr(result, 'final'):
                    result.final = FinalType(flag)
                else:
                    log.warning('Got unexpected final flag')
        if envelope_data:
            raise ProtocolError(f'extra keys found: {envelope_data.keys()}')
        return result
    
    def _get_request_response_type(self, cls: type, type_flags: set) -> Tuple[str, Union[RequestType, ResponseType]]:
        if len(type_flags) != 1:
            raise ValueError('Invalid type_flags state')
        type_chr = type_flags.pop()
        if cls is Request:
            return 'request_type', RequestType(type_chr)
        if cls is Response:
            return 'response_type', ResponseType(type_chr)
        raise ValueError(f'Invalid cls of {cls!s}')

    # def get_system_register(self):
    #     register = FunctionRegister(namespace='#sys')
    #     register.register_func(self._ping, 'ping')
    #     register.register_func(self.get_engine_signature, 'get_sig')
    #     return register
    
    # def get_engine_signature(self):
    #     return self.TAG
    
    # def _ping(self):
    #     return 'pong'
