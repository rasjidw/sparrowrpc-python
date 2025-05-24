from __future__ import annotations

try:
    from dataclasses import dataclass
except ImportError:
    from udataclasses import dataclass  # type: ignore (for micropython)

from typing import Any, Iterable, Optional
import sys

if sys.version_info >= (3, 11) or sys.implementation.name == 'micropython':
    from enum import StrEnum
else:
    from backports.strenum import StrEnum # type: ignore


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
    iter: Optional[Iterable] = None
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
        normal_params = set(self.params.keys()) if self.params else set()
        cb_params = set(self.callback_params.keys()) if self.callback_params else set()
        duplicates = normal_params.intersection(cb_params)
        if duplicates:
            raise ValueError(f'duplicate params found: {duplicates}')

        # FIXME: Do we will need this restriction now we are event based?
        if self.request_type == RequestType.SILENT and cb_params:
            raise ValueError("Can't have callbacks on Slient Requests")


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
    param_details: Optional[list[str]] = None   # None = unspecified. FIXME: just using None for now. Do we want to send types too? 


@dataclass
class IterableCallbackInfo:
    iter: Iterable = None
    return_details: Any = None   # FIXME: This is not done yet.