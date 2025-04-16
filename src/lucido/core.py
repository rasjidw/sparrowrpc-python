from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
import inspect
import logging
import sys
from typing import Any


if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum


'''
Module contains all the core data structures used by both threaded and asyncio engines.
'''


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


@dataclass
class RequestCallbackInfo:
    func: callable
    param_details: list[str]|None = None   # None = unspecified. FIXME: just using None for now. Do we want to send types too? 


@dataclass
class IterableCallbackInfo:
    iter: Iterable
    return_details: Any = None   # FIXME: This is not done yet.



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
        

