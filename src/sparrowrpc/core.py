from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
try:
    from dataclasses import dataclass
except ImportError:
    from udataclasses import dataclass  # type: ignore (for micropython)
import inspect
import logging
import sys
from typing import Any, Iterable


try:
    from enum import StrEnum
except ImportError:
    # FIXME: are we even supporting versions of Python where this is required?
    from backports.strenum import StrEnum # type: ignore

from binarychain import BinaryChain


'''
Module contains all the core data structures used by both threaded and asyncio engines.
'''


log = logging.getLogger(__name__)


class RequestType(StrEnum):
    NORMAL = ''
    QUIET = 'q'    # returns an empty response after the call is completed, unless there is an exception. (Like normal, but flagging that we don't want the result.)
    SILENT = 's'   # returns an empty response after call is *delivered*, unless there is an exception (transport error / invalid target).
    

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
class PushIterableInfo:
    iter: Iterable = None
    return_details: Any = None  # FIXME: This is not done yet.



@dataclass
class RequestBase:
    target: str = ''
    namespace: str = ''
    node: str = None
    params: dict = None  # param name -> value
    callback_request_id: int = None
    raw_binary: bool = False
    data: bytearray = None  # only set / valid if raw_binary = True

    def __post_init__(self):
        # convert namespace of None to ''
        self.namespace = '' if self.namespace is None else self.namespace
        self.validate()

    def validate(self):
        if not self.target:
            raise ValueError('target must be set')

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
class ResponseBase:
    request_id: int = None   # default required for micropython
    response_type: ResponseType = ResponseType.NORMAL
    acknowledge: bool = False
    raw_binary: bool = False
    final: FinalType = None   # only relevent to multipart responses


@dataclass
class OutgoingResponse(ResponseBase):
    result: Any = None


@dataclass
class MessageSentEvent:
    request_id: int = None


@dataclass
class AcknowledgeBase:
    request_id: int = None


class OutgoingAcknowledge(AcknowledgeBase):
    pass


class IncomingAcknowledge(AcknowledgeBase):
    pass


class MtpeExceptionCategory(StrEnum):
    CALLER = 'r'  # the caller made an error (invalid param value or type, not authorised, target not found)
    CALLEE = 'e'  # a callee error occured
    TRANSPORT = 't'  # some kind of transport error, or parse error.  # FIXME: Do we need this? Does this make sense?




@dataclass
class MtpeExceptionInfo:
    category: MtpeExceptionCategory = None
    type: str = None
    msg: str = None
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
    msg: str = ''
    data: bytes = b''
    def __post_init__(self):
        self.msg.encode('ascii')  # raises an exception of not a valid ControMsg (ie, non ascii)


@dataclass
class RequestCallbackInfo:
    func: callable = None
    param_details: list[str]|None = None   # None = unspecified. FIXME: just using None for now. Do we want to send types too? 


@dataclass
class IterableCallbackInfo:
    iter: Iterable = None
    return_details: Any = None   # FIXME: This is not done yet.



@dataclass
class FuncInfo:
    target_name: str = ''
    namespace: str = ''
    auth_groups: list[str] = None  # FIXME: maybe something more general, like tags.
    multipart_response: bool = False
    func: callable = None
    iterable_callback: Iterable = None   # only one of func or iterable_callback should be set
    injectable_params: dict = None       # param name to callable that returns the injected param.


class FunctionRegister:
    def __init__(self, namespace: str = None):
        self.namespace = namespace if namespace else ''
        self._register = dict()   # dict[namespace][target_name] -> FuncInfo
    def register_func(self, func, target_name=None, namespace=None, auth_groups=None, multipart_response=False, injectable_params=None):
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
            target_name = func_data['__name__']
        if auth_groups is None:
            auth_groups = []
        if namespace not in self._register:
            self._register[namespace] = dict()
        func_info = FuncInfo(target_name=target_name, namespace=namespace, auth_groups=auth_groups, multipart_response=multipart_response, func=func,
                             injectable_params=injectable_params)
        if target_name in self._register[namespace]:
            raise ValueError(f'duplicate registration of {target_name} into "{namespace}" namespace')
        self._register[namespace][target_name] = func_info
        log.debug(f'Registered {func_info}')

    def get_method_info(self, target_name, namespace=None) -> FuncInfo:
        try:
            return self._register[namespace][target_name]
        except KeyError:
            return None

default_func_register = FunctionRegister()



class MsgChannelRegister:
    def __init__(self):
        self.channel_register = defaultdict(set)  # tag -> set of MsgChannels

    def register(self, msg_channel: MsgChannelBase):
        self.channel_register[msg_channel.tag].add(msg_channel)

    def unregister(self, msg_channel: MsgChannelBase):
        self.channel_register[msg_channel.tag].remove(msg_channel)
        
    def get_channels_by_tag(self, tag):
        return frozenset(self.channel_register[tag])


global_channel_register = MsgChannelRegister()


class MsgChannelBase:
    def __init__(self, initiator: bool, engine: ProtocolEngineBase, channel_tag='', func_registers=None, channel_register=None):
        self.initiator = initiator
        self.engine = engine
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
        if isinstance(message, OutgoingRequest):
            add_id = True
            register_event_callback = True
        if isinstance(message, OutgoingAcknowledge):
            # can't add ids to outgoing acknowledge
            add_id = False
        if isinstance(message, OutgoingResponse):
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

    def _get_func_info_and_ack_err_msg(self, message: IncomingRequest|IncomingNotification):
        ack_err_msg = None
        func_info = self._lookup_func_register(message.target, message.namespace)
        if func_info:
            assert isinstance(func_info, FuncInfo)
            if isinstance(message, IncomingRequest) and message.acknowledge:
                if message.request_type == RequestType.SILENT:
                    log.warning(f'Silent Request {message} flagged with Acknowledge')
                else:
                    if message.id is None:
                        log.error(f'Incoming request without an id')
                    else:
                        ack_err_msg = OutgoingAcknowledge(request_id=message.id)
        else:
            if isinstance(message, IncomingRequest):
                exc_info = MtpeExceptionInfo(MtpeExceptionCategory.CALLER, type='TargetNotFound', msg=f'target {message.target} not found')
                ack_err_msg = OutgoingException(message.id, exc_info=exc_info)
        return func_info, ack_err_msg
    
    def _parse_and_allocate_bin_chain(self, bin_chain):
        dispatch = False
        incoming_callback = None
        log.debug(f'Got incoming binary chain: {repr(bin_chain)}')
        message = self.engine.parse_incoming_message(bin_chain)
        log.debug(f'Got incoming message {message}')

        if (isinstance(message, IncomingRequest) or isinstance(message, IncomingNotification)) and not message.callback_request_id:
            dispatch = True
        else:
            request_completed = False
            if (isinstance(message, IncomingRequest) or isinstance(message, IncomingNotification)) and message.callback_request_id:
                request_id = message.callback_request_id
            else:
                request_id = message.request_id
            if isinstance(message, IncomingResponse):
                if message.response_type == ResponseType.NORMAL:
                    request_completed = True
                elif message.response_type == ResponseType.MULTIPART:
                    request_completed = message.final
                else:
                    raise RuntimeError(f'Invalid response type of {message.response_type!r}')
            if isinstance(message, IncomingException):
                request_completed = True
            try:
                if request_completed:
                    incoming_callback = self._message_event_callbacks.pop(request_id)
                else:
                    incoming_callback = self._message_event_callbacks[request_id]
            except KeyError:
                log.warning(f'Incoming message {message} with invalid request id')
        return message, dispatch, incoming_callback


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



# decorators

def make_export_decorator(defaul_namespace=None):
    return ExportDecorator(defaul_namespace)


class ExportDecorator:
    def __init__(self, default_namespace='', func_register = None):
        self.default_namespace = default_namespace
        self.register = func_register if func_register else default_func_register
        assert isinstance(self.register, FunctionRegister)

    def __call__(self, _func=None, target_name=None, namespace=None, auth_groups=None, multipart_response=False, injectable_params=None):
        def decorate(func):
            self.register.register_func(func, target_name, namespace, auth_groups, multipart_response, injectable_params=injectable_params)
            return func
        if _func and callable(_func):
            return decorate(_func)
        else:
            return decorate
        

