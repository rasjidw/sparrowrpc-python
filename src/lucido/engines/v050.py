import logging

from binarychain import BinaryChain

from ..core import FunctionRegister
from ..serialisers import BaseSerialiser

from ..core import ProtocolEngineBase
from ..core import OutgoingAcknowledge, OutgoingException, OutgoingNotification, OutgoingRequest, OutgoingResponse
from ..core import IncomingAcknowledge, IncomingException, IncomingNotification, IncomingRequest, IncomingResponse
from ..core import ControlMsg, RequestBase, RequestCallbackInfo, RequestType, ResponseType, MtpeExceptionCategory, MtpeExceptionInfo, IterableCallbackInfo
from ..core import RequestType, ResponseType, FinalType
from ..threaded import IterableCallbackProxy, CallbackProxy

from ..exceptions import ProtocolError


log = logging.getLogger(__name__)


class ProtocolEngine(ProtocolEngineBase):
    _sig = 'V0.5'
    max_bc_length = 3   # envelope, params or result, callback params (prefix not counted)
    envelope_marker_map = {'r': (IncomingRequest, 'target', set('qsm')),
                           'n': (IncomingNotification, 'target', None),
                           'p': (IncomingResponse, 'request_id', set('m')),
                           'x': (IncomingException, 'request_id', None),
                           'a': (IncomingAcknowledge, 'request_id', None)
                           }
    envelope_unique = set(envelope_marker_map.keys())
    envelope_sup = {'i': 'id',
                    's': 'namespace',
                    'd': 'node',
                    'c': 'callback_request_id',
                    }
    flags = {'a': 'acknowledge', 'b': 'raw_binary'}
    def __init__(self, serialiser: BaseSerialiser, always_send_ids=False):
        self.serialiser = serialiser
        self.always_send_ids = always_send_ids  # if true, always add ids. Otherwise just when needed (requests, or when an acknowledgement is requested).
        self.message_id = 1

        self._make_map = {OutgoingRequest: self._make_out_request,
                          OutgoingNotification: self._make_out_notification,
                          OutgoingResponse: self._make_out_resp,
                          OutgoingException: self._make_out_except,
                          OutgoingAcknowledge: self._make_out_ack,
                          ControlMsg: self._make_control,
                          }
        
        self.channels = list()

    def outgoing_message_to_binary_chain(self, message: RequestBase, message_id: int):
        try:
            make_method = self._make_map[type(message)]
            prefix, msg_parts = make_method(message, message_id)
        except KeyError:
            raise TypeError(f'Invalid message type {type(message)}')
        return BinaryChain(prefix, msg_parts)

    def _make_out_request(self, message: OutgoingRequest, message_id: int):
        if message_id is None:
            raise ValueError('message id is required on Outgoing Requests')
        marker_chrs = ['r']
        if message.request_type == RequestType.SILENT and message.acknowledge:
            raise ValueError('acknowledge can not use set on SILENT requests.')
        if message.acknowledge:
            marker_chrs.append('a')
        if message.raw_binary:
            marker_chrs.append('b')
        if message.request_type != RequestType.NORMAL:
            marker_chrs.append(str(message.request_type))
        envelope_data = {''.join(marker_chrs): message.target, 'i': message_id}
        if message.callback_request_id:
            envelope_data['c'] = message.callback_request_id
        if message.node:
            envelope_data['d'] = message.node
        if message.namespace:
            envelope_data['s'] = message.namespace
        msg_parts = [self.serialiser.serialise(envelope_data)]
        if message.params or message.callback_params:
            param_part = self.serialiser.serialise(message.params) if message.params else b''
            msg_parts.append(param_part)

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
                special_params_part = self.serialiser.serialise(special_params)
                msg_parts.append(special_params_part)
        return '', msg_parts
        
    def _make_out_notification(self, message: OutgoingNotification, message_id: int|None):
        marker_chrs = ['n']
        if message.raw_binary:
            marker_chrs.append('b')
        envelope_data = {''.join(marker_chrs): message.target}
        if message_id:  # message_id is optional for Notifications, but can be assigned for logging purposes etc
            envelope_data['i'] = message_id             
        if message.node:
            envelope_data['d'] = message.node
        if message.namespace:
            envelope_data['s'] = message.namespace
        if message.callback_request_id:
            envelope_data['c'] = message.callback_request_id
        msg_parts = [self.serialiser.serialise(envelope_data)]
        if message.params:
            param_part = self.serialiser.serialise(message.params) if message.params else b''
            msg_parts.append(param_part)
        return '', msg_parts

    def _make_out_resp(self, message: OutgoingResponse, message_id: int):
        marker = 'p'
        marker += message.response_type
        if message.final:
            marker += message.final
        if message.acknowledge:
            marker += 'a'
        envelope_data = {marker: message.request_id}
        if message_id is not None:
            envelope_data['i'] = message_id
        msg_parts = [self.serialiser.serialise(envelope_data)]
        if message.result is not None:  # FIXME? Do we need to allow returning of None, and use Void instead?
            msg_parts.append(self.serialiser.serialise(message.result))
        return '', msg_parts

    def _make_out_except(self, message: OutgoingException, message_id: int):
        marker = 'x'
        if message.acknowledge:
            marker += 'a'
        envelope_data = {marker: message.request_id}
        if message_id is not None:
            envelope_data['i'] = message_id
        exc_info = message.exc_info
        except_data = {'cat': str(exc_info.category), 'type': exc_info.type}
        if exc_info.msg:
            except_data['msg'] = exc_info.msg
        if exc_info.details:
            except_data['details'] = exc_info.details
        if exc_info.value is not None:
            except_data['value'] = exc_info.value
        msg_parts = [self.serialiser.serialise(envelope_data), self.serialiser.serialise(except_data)]
        return '', msg_parts
    
    def _make_out_ack(self, message: OutgoingAcknowledge, message_id: int):
        envelope_data = {'a': message.request_id}
        msg_parts = [self.serialiser.serialise(envelope_data)]
        return '', msg_parts

    def _make_control(self, control_msg: ControlMsg):
        return control_msg.msg, None
    
    def parse_incoming_envelope(self, incoming_bin_chain: BinaryChain):
        msg_parts = incoming_bin_chain.parts
        if len(msg_parts) == 0:
            return ControlMsg(incoming_bin_chain.prefix), None
        if incoming_bin_chain.prefix == '' and len(msg_parts) <= self.max_bc_length:
            raw_envelope = msg_parts[0]
            raw_contents = msg_parts[1:]
            envelope = self._parse_raw_envelope(raw_envelope)
            return envelope, raw_contents
        raise ProtocolError('invalid binary chain')
    
    def parse_incoming_message(self, incoming_bin_chain: BinaryChain):
        message, raw_contents = self.parse_incoming_envelope(incoming_bin_chain)
        if not raw_contents:
            return message
        if isinstance(message, IncomingRequest):
            if message.raw_binary:
                message.data = raw_contents[0]
                return message
            
            # normal params (not raw binary)
            if raw_contents[0]:
                message.params = self.serialiser.deserialise(raw_contents[0])
            if len(raw_contents) > 1:
                special_params_dict = self.serialiser.deserialise(raw_contents[1])  # cb_param -> target_name
                cb_params = dict()
                raw_std_cb_params = special_params_dict.get('#cb', dict())
                for (param_name, info) in raw_std_cb_params.items():   # FIXME: currently ignoring info
                    cb_params[param_name] = CallbackProxy(param_name, message.id)
                raw_iter_cb_params = special_params_dict.get('#icb', dict())
                for (param_name, info) in raw_iter_cb_params.items():
                    cb_params[param_name] = IterableCallbackProxy(param_name, message.id)
                message.callback_params = cb_params
            return message
        if isinstance(message, IncomingNotification):
            if message.raw_binary:
                message.data = raw_contents[0]
                return message
            
            # normal params (not raw binary)
            message.params = self.serialiser.deserialise(raw_contents[0])
            return message
        if isinstance(message, IncomingResponse):
            if message.raw_binary:
                message.result = raw_contents[0]
            else:
                message.result = self.serialiser.deserialise(raw_contents[0])
            return message
        if isinstance(message, IncomingException):
            raw_except_data = self.serialiser.deserialise(raw_contents[0])
            message.exc_info = MtpeExceptionInfo(MtpeExceptionCategory(raw_except_data['cat']), raw_except_data['type'], raw_except_data['msg'])
            return message
        raise RuntimeError('invalid message passed in')

    def _parse_raw_envelope(self, envelope_data: bytes):
        data = self.serialiser.deserialise(envelope_data)
        if not isinstance(data, dict):
            raise ProtocolError('envelope must be a dict')
        key_type_to_flags = {key[0]: key[1:] for key in data.keys()}
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
        kwargs = {attr: data.pop(actual_key)}
        for key, attr in self.envelope_sup.items():
            if key in data:
                kwargs[attr] = data.pop(key)
        if type_flags:
            attr, value = self._get_request_response_type(cls, type_flags)
            kwargs[attr] = value
        result = cls(**kwargs)
        for flag in flags:
            if flag == 'a':
                if hasattr(result, 'acknowledge'):
                    result.acknowledge = True
                else:
                    log.warning('Got unexpected acknowledge flag')  # FIXME: make these warning protocol errors?
            elif flag == 'b':
                if isinstance(result, IncomingException) or isinstance(result, IncomingAcknowledge):
                    raise ProtocolError('binary flag not valid on exceptions or acks')
                if hasattr(result, 'raw_binary'):
                    result.raw_binary = True
                else:
                    log.warning('Got unexpected acknowledge raw binary flag')
            elif flag in ('f', 't'):
                if hasattr(result, 'final'):
                    result.final = FinalType(flag)
                else:
                    log.warning('Got unexpected final flag')
        if data:
            raise ProtocolError(f'extra keys found: {data.keys()}')
        return result
    
    def _get_request_response_type(self, cls: type, type_flags: set):
        if len(type_flags) != 1:
            raise ValueError('Invalid type_flags state')
        type_chr = type_flags.pop()
        if cls is IncomingRequest:
            return 'request_type', RequestType(type_chr)
        if cls is IncomingResponse:
            return 'response_type', ResponseType(type_chr)

    def get_system_register(self):
        register = FunctionRegister(namespace='#sys')
        register.register_func(self._ping, 'ping')
        register.register_func(self.get_engine_signature, 'get_sig')
        return register
    
    def get_engine_signature(self):
        return f'{self._sig}/{self.serialiser.sig}'.lower()
    
    def _ping(self):
        return 'pong'
