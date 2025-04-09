from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
import inspect
import json
import logging
from typing import Any
import sys

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum


from msgpack import packb, unpackb
from binarychain import BinaryChain


log = logging.getLogger(__name__)


class RequestType(StrEnum):
    NORMAL = ''
    QUIET = 'q'    # returns an empty response after the call is completed, unless there is an exception. (Like normal, but flagging that we don't want the result.)
    SILENT = 's'   # returns an empty response after call is *delivered*, unless there is an exception (transport error / invalid target).
    MUTIPART = 'm' # multipart request - will be followed by linked messages.
    

class ResponseType(StrEnum):
    NORMAL = ''      # single response only
    MULTIPART = 'm'  # may return multiple reponse messages. The last one is flagged as 'final'  - either FINAL (has data) or TERMINATOR (no data)


class FinalType(StrEnum):
    FINAL = 'f'       # final - with data
    TERMINATOR = 't'  # terminator - no data


# May no longer need this?

# # Python makes no destinction between a function that has no return value,
# # and a fuction that returns None.
# # Add this if it makes it more consistent with other languages.

# class VoidType:
#     def __repr__(self):
#         return 'Void'


# Void = VoidType()


@dataclass
class RequestCallbackInfo:
    func: callable
    param_details: list[str]|None = None   # None = unspecified. FIXME: just using None for now. Do we want to send types too? 


class CallbackProxyBase:
    def __init__(self, cb_param_name, callback_request_id):
        self._cb_param_name = cb_param_name
        self._callback_request_id = callback_request_id
        self._channel = None

    def set_channel(self, channel):
        # FIXME: create base MsgChannel in core so we can assert this is a MsgChannel here.
        self._channel = channel


class CallbackProxy(CallbackProxyBase):
    def __init__(self, cb_param_name, callback_request_id):
        CallbackProxyBase.__init__(self, cb_param_name, callback_request_id)
        self.request_type = None  # can be changed before sending / FIXME: how we do set a default?

    def set_to_notification(self):
        self.request_type = None

    def set_to_request(self, request_type: RequestType):
        self.request_type = request_type

    def __call__(self, **kwargs):
        if self.request_type:
            outgoing_msg = OutgoingRequest(target=self._cb_param_name, callback_request_id=self._callback_request_id, params=kwargs, request_type=self.request_type)
        else:
            outgoing_msg = OutgoingNotification(target=self._cb_param_name, callback_request_id=self._callback_request_id, params=kwargs)
        log.debug(f'Callback Proxy call: {repr(outgoing_msg)}')
        proxy = self._channel.get_proxy()
        if self.request_type:
            result = proxy.send_request(outgoing_msg)
            return result
        else:
            proxy.send_notification(outgoing_msg)


@dataclass
class IterableCallbackInfo:
    iter: Iterable
    return_details: Any = None   # FIXME: This is not done yet.


class IterableCallbackProxy(Iterable, CallbackProxyBase):
    def __init__(self, cb_param_name, callback_request_id):
        CallbackProxyBase.__init__(self, cb_param_name, callback_request_id)
        self._final = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._final:
            raise StopIteration()
        
        outgoing_msg = OutgoingRequest(target=self._cb_param_name, callback_request_id=self._callback_request_id)
        log.debug(f'CallbackIterableProxy call: {repr(outgoing_msg)}')
        proxy = self._channel.get_proxy()
        result, final = proxy.send_request_for_iter(outgoing_msg)
        if final == FinalType.TERMINATOR:
            raise StopIteration()
        elif final == FinalType.FINAL:
            self._final = True
        return result


@dataclass
class PushIterableInfo:
    iter: Iterable
    return_details: Any = None  # FIXME: This is not done yet.



@dataclass
class RequestBase:
    target: str
    namespace: str = ''
    node: str = None
    params: dict = None  # param name -> value
    callback_request_id: int = None
    raw_binary: bool = False
    data: bytearray = None  # only set / valid if raw_binary = True

    def __post_init__(self):
        # convert namespace of None to ''
        self.namespace = '' if self.namespace is None else self.namespace


@dataclass
class OutgoingRequest(RequestBase):
    acknowledge: bool = False
    request_type: RequestType = RequestType.NORMAL
    callback_params: dict = None  # param name -> CallbackInfo

    def __post_init__(self):
        self.validate()

    def validate(self):
        # FIXME: Do we will need this restriction now we are event based?
        if self.request_type == RequestType.SILENT and cb_params:
            raise ValueError("Can't have callbacks on Slient Requests")
        
        normal_params = set(self.params.keys()) if self.params else set()
        cb_params = set(self.callback_params.keys()) if self.callback_params else set()
        duplicates = normal_params.intersection(cb_params)
        if duplicates:
            raise ValueError(f'duplicate params found: {duplicates}')        


# Notifications are requests that should never return a response (even in the case of errors at the remote end)
class OutgoingNotification(RequestBase):
    pass


@dataclass
class OutgoingLinkedMessage:
    request_id: int
    data: Any = None
    acknowledge: bool = False
    raw_binary: bool = False
    final: FinalType = None


@dataclass
class IncomingLinkedMessage(OutgoingLinkedMessage):
    id: int = None


@dataclass
class ResponseBase:
    request_id: int
    response_type: ResponseType = ResponseType.NORMAL
    acknowledge: bool = False
    raw_binary: bool = False
    final: FinalType = None   # only relevent to multipart responses


@dataclass
class OutgoingResponse(ResponseBase):
    result: Any = None


@dataclass
class MessageSentEvent:
    request_id: int


@dataclass
class AcknowledgeBase:
    request_id: int


class OutgoingAcknowledge(AcknowledgeBase):
    pass


class IncomingAcknowledge(AcknowledgeBase):
    pass


class MtpeExceptionCategory(StrEnum):
    CALLER = 'r'  # the caller made an error (invalid param value or type, not authorised, target not found)
    CALLEE = 'e'  # a callee error occured
    TRANSPORT = 't'  # some kind of transport error, or parse error.  # FIXME: Do we need this? Does this make sense?


class CallerException(Exception):
    @classmethod
    def get_subclasses(cls):
        for subclass in cls.__subclasses__():
            yield from subclass.get_subclasses()
            yield subclass


class TargetNotFound(CallerException):
    pass


class InvalidParams(CallerException):
    pass


class CalleeException(Exception):
    pass


@dataclass
class MtpeExceptionInfo:
    category: MtpeExceptionCategory
    type: str
    msg: str
    details: str = ''
    value: int = None

@dataclass
class OutgoingException(ResponseBase):
    exc_info: MtpeExceptionInfo = None

    def __post_init__(self):
        if self.exc_info is None:
            raise ValueError('exc_info is required')


@dataclass
class IncomingRequest(OutgoingRequest):
    id: int = None


@dataclass
class IncomingNotification(OutgoingNotification):
    id: int = None


@dataclass
class IncomingResponse(ResponseBase):
    id: int = None
    result: Any = None

@dataclass
class IncomingException(ResponseBase):
    id: int = None
    exc_info: MtpeExceptionInfo = None


@dataclass(frozen=True)
class ControlMsg:
    msg: str
    data: bytes = b''
    def __post_init__(self):
        self.msg.encode('ascii')  # raises an exception of not a valid ControMsg (ie, non ascii)


@dataclass
class FuncInfo:
    target_name: str
    namespace: str
    auth_groups: list[str]  # FIXME: maybe something more general, like tags.
    multipart_request: str   # argument name to use as the incoming iterator / generator  # FIXME: Probably remove this.
    multipart_reponse: bool
    func: callable
    is_iterable_callback: bool = False   # currently only used for iterable callbacks - not sure if it makes sense elsewhere
    injectable_params: dict = None       # param name to callable that returns the injected param.


class ProtocolError(Exception):
    pass


class FunctionRegister:
    def __init__(self, namespace: str = None):
        self.namespace = namespace if namespace else ''
        self._register = dict()   # dict[namespace][target_name] -> FuncInfo
    def register_func(self, func, target_name=None, namespace=None, auth_groups=None, multipart_request=None, multipart_response=False, injectable_params=None):
        namespace = '' if namespace is None else namespace
        if not isinstance(namespace, str):
            raise TypeError('namespace must be a string')
        if self.namespace:
            if namespace:
                raise ValueError('namespace pre-set for this register')
            namespace = self.namespace
        else:
            if namespace == '#sys':
                raise ValueError(f'{namespace} is reserved and cannot be used in this register')
        if not target_name:
            func_data = dict(inspect.getmembers(func))
            target_name = func_data['__qualname__']
        if auth_groups is None:
            auth_groups = []
        if namespace not in self._register:
            self._register[namespace] = dict()
        func_info = FuncInfo(target_name, namespace, auth_groups, multipart_request, multipart_response, func, injectable_params=injectable_params)
        if target_name in self._register[namespace]:
            raise ValueError(f'duplicate registration of {target_name} into "{namespace}" namespace')
        self._register[namespace][target_name] = func_info
        log.debug(f'Registered {func_info}')

    def get_method_info(self, target_name, namespace=None) -> FuncInfo:
        try:
            return self._register[namespace][target_name]
        except KeyError:
            return None

    

class BaseSerialiser(ABC):
    sig = None
    @abstractmethod
    def serialise(self, obj_data: Any) -> bytes:
        raise NotImplementedError()
    @abstractmethod
    def deserialise(self, bin_data: bytes) -> Any:
        raise NotImplementedError()


class MsgpackSerialiser(BaseSerialiser):
    sig = 'MP'
    def serialise(self, obj_data: Any) -> bytes:
        return packb(obj_data)
    def deserialise(self, bin_data: bytes) -> Any:
        return unpackb(bin_data)


class JsonSerialiser(BaseSerialiser):
    sig = 'JS'
    def serialise(self, obj_data: Any) -> bytes:
        return json.dumps(obj_data).encode()
    def deserialise(self, bin_data: bytes) -> Any:
        return json.loads(bin_data.decode())


class ProtocolEngineBase(ABC):
    @abstractmethod
    def outgoing_message_to_binary_chain(self, message: RequestBase, message_id: int):
        raise NotImplementedError()
    
    @abstractmethod
    def parse_incoming_envelope(self, incoming_bin_chain: BinaryChain):
        raise NotImplementedError()
    
    @abstractmethod
    def parse_incoming_message(self, incoming_bin_chain: BinaryChain):
        raise NotImplementedError()


class ProtocolEngine(ProtocolEngineBase):
    _sig = 'V0.5'
    max_bc_length = 3   # envelope, params or result, callback params (prefix not counted)
    envelope_marker_map = {'r': (IncomingRequest, 'target', set('qsm')),
                           'n': (IncomingNotification, 'target', None),
                           'p': (IncomingResponse, 'request_id', set('m')),
                           'x': (IncomingException, 'request_id', None),
                           'l': (IncomingLinkedMessage, 'request_id', None),
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
                          OutgoingLinkedMessage: self._make_out_linked,
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
    
    def _make_out_linked(self, message: OutgoingLinkedMessage, message_id: int|None):
        marker = 'l'
        if message.acknowledge:
            marker += 'a'
        if message.raw_binary:
            marker += 'b'
        if message.final:
            marker += message.final
        envelope_data = {marker: message.request_id}
        if message_id:
            envelope_data['i'] = message_id
        msg_parts = [self.serialiser.serialise(envelope_data)]
        if message.data:
            param_part = self.serialiser.serialise(message.data) if message.data else b''
            msg_parts.append(param_part)
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
        if isinstance(message, IncomingLinkedMessage):
            if message.raw_binary:
                message.data = raw_contents[0]
            else:
                message.data = self.serialiser.deserialise(raw_contents[0]) if raw_contents[0] else None
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


default_func_register = FunctionRegister()

# decorators

def make_export_decorator(defaul_namespace=None):
    return ExportDecorator(defaul_namespace)


class ExportDecorator:
    def __init__(self, default_namespace='', func_register = None):
        self.default_namespace = default_namespace
        self.register = func_register if func_register else default_func_register
        assert isinstance(self.register, FunctionRegister)
    def __call__(self, _func=None, target_name=None, namespace=None, auth_groups=None, multipart_request=None, multipart_response=False, injectable_params=None):
        def decorate(func):
            self.register.register_func(func, target_name, namespace, auth_groups, multipart_request, multipart_response, injectable_params=injectable_params)
            return func
        if _func and callable(_func):
            return decorate(_func)
        else:
            return decorate
        

class InjectorBase(ABC):    
    # FIXME: create base MsgChannel in core so we can assert this is a MsgChannel here.
    def __init__(self, msg_channel, incoming_request: RequestBase, func_info: FuncInfo):
        self.msg_channel = msg_channel
        self.incoming_request = incoming_request
        self.func_info = func_info

    @abstractmethod
    def pre_call_setup(self):
        raise NotImplementedError
    
    @abstractmethod
    def get_param(self):
        raise NotImplementedError
    
    @abstractmethod
    def post_call_cleanup(self):
        raise NotImplementedError


class MsgChannelInjector(InjectorBase):
    def pre_call_setup(self):
        pass

    # FIXME: create base MsgChannel in core so we can assert this is a MsgChannel here.
    def get_param(self):
        return self.msg_channel
        
    def post_call_cleanup(self):
        pass
