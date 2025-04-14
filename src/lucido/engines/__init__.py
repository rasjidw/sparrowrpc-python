from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
import logging
import sys
from typing import Any

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum

from binarychain import BinaryChain

from ..core import FuncInfo


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
