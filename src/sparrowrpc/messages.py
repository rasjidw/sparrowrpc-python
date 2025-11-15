from __future__ import annotations

from typing import Any, Iterable, Optional, Callable, Dict
import sys

if sys.version_info >= (3, 11) or sys.implementation.name == 'micropython':
    from enum import StrEnum
else:
    from backports.strenum import StrEnum # type: ignore


__all__ = ['RequestType', 'ResponseType', 'FinalType', 'PushIterableInfo', 'MessageBase', 'CallBase', 'Request',
           'Notification', 'ResponseBase', 'Response', 'MessageSentEvent', 'Acknowledge',
           'ExceptionCategory', 'ExceptionInfo', 'ExceptionResponse', 'RequestCallbackInfo', 'IterableCallbackInfo']


class RequestType(StrEnum):
    NORMAL = ''
    QUIET = 'q'    # returns an empty response after the call is completed, unless there is an exception.
                   # (Like normal, but flagging that we don't want the result.)
    SILENT = 's'   # returns an empty response after call is *delivered*,
                   # unless there is an exception (transport error / invalid target).


class ResponseType(StrEnum):
    NORMAL = ''      # single response only
    MULTIPART = 'm'  # may return multiple response messages. The last one is flagged as 'final'
                     # - either FINAL (has data) or TERMINATOR (no data)


class FinalType(StrEnum):
    FINAL = 'f'       # final - with data
    TERMINATOR = 't'  # terminator - no data


# May no longer need this?

# # Python makes no distinction between a function that has no return value,
# # and a function that returns None.
# # Add this if it makes it more consistent with other languages.

# class VoidType:
#     def __repr__(self):
#         return 'Void'


# Void = VoidType()


class PushIterableInfo:
    def __init__(self, iter: Optional[Iterable] = None, return_details: Any = None):
        self.iter = iter
        self.return_details = return_details     # FIXME: return_details is not done yet.


class EventBase:
    _prop_cache = None

    @property
    def properties(self):
        if self._prop_cache is None:
            # FIXME?: Is this safe under free threading?
            self._prop_cache = sorted([key for key in self.__dict__.keys() if not key.startswith('_')])
        return self._prop_cache
    
    def __repr__(self):
        items = [f"{prop}={getattr(self, prop)!r}" for prop in self.properties]
        if items:
            return f"<{self.__class__.__name__}: {', '.join(items)}>"
        else:
            return f"<{self.__class__.__name__}>"
        
    def as_dict(self):
        return {(prop, getattr(self, prop)) for prop in self.properties}
        
    def __eq__(self, other: EventBase):
        if type(self) == type(other):
            return self.as_dict() == other.as_dict()
        else:
            return False


class TransportEvent(EventBase):
    pass


class MessageSentEvent(TransportEvent):
    def __init__(self, request_id: int):
        self.request_id = request_id


class TransportClosedEvent(TransportEvent):
    def __init__(self, closing_end: str):
        # FIXME: Use a StrEnum?
        if closing_end not in ('remote', 'local'):
            raise ValueError()
        self.closing_end = closing_end
        
    def raise_as_exception(self):
        raise TransportClosedError(self.closing_end)


class TransportBrokenEvent(TransportEvent):
    def __init__(self, original_exc: Exception):
        self.original_exc = original_exc

    def raise_as_exception(self):
        raise TransportBrokenError(self.original_exc)


class TransportClosedError(TransportClosedEvent, Exception):
    pass


class TransportBrokenError(TransportBrokenEvent, Exception):
    pass


class MessageBase(EventBase):
    def __init__(self, envelope_serialisation_code: Optional[str], protocol_version: Optional[str]):
        self.envelope_serialisation_code = envelope_serialisation_code
        self.protocol_version = protocol_version


class CallBase(MessageBase):
    def __init__(self, target: str, namespace: str='', node: Optional[str]=None,
                 params: Optional[Dict]=None,  # param name -> value
                 callback_request_id: Optional[int]=None, envelope_serialisation_code: Optional[str]=None,
                 protocol_version: Optional[str]=None, payload_serialisation_code: Optional[str]=None,
                 ):
        MessageBase.__init__(self, envelope_serialisation_code, protocol_version)
        self.target = target
        self.namespace = '' if not namespace else namespace  # FIXME: Review if None is better as the default
        self.node = node
        self.params = params
        self.payload_serialisation_code = payload_serialisation_code
        self.callback_request_id = callback_request_id


class Request(CallBase):
    def __init__(self, target: str, namespace: str='', node: Optional[str]=None,
                 params: Optional[dict]=None,  # param name -> value
                 callback_request_id: Optional[int]=None, envelope_serialisation_code: Optional[str]=None,
                 protocol_version: Optional[str]=None,
                 payload_serialisation_code: Optional[str]=None, 
                 acknowledge: bool=False, request_type: RequestType=RequestType.NORMAL, 
                 callback_params: Optional[Dict[str, RequestCallbackInfo]]=None,  # param name -> CallbackInfo
                 message_id: Optional[int]=None
                 ):
        CallBase.__init__(self, target, namespace, node, params, callback_request_id, envelope_serialisation_code,
                          protocol_version, payload_serialisation_code)
        self.acknowledge = acknowledge
        self.request_type = request_type
        self.callback_params = callback_params
        self.message_id = message_id

        normal_params = set(self.params.keys()) if self.params else set()
        cb_params = set(self.callback_params.keys()) if self.callback_params else set()
        duplicates = normal_params.intersection(cb_params)
        if duplicates:
            raise ValueError(f'duplicate params found: {duplicates}')

        # FIXME: Do we will need this restriction now we are event based?
        if self.request_type == RequestType.SILENT and cb_params:
            raise ValueError("Can't have callbacks on Slient Requests")


# Notifications are requests that should never return a response (even in the case of errors at the remote end)
class Notification(CallBase):
    pass


class ResponseBase(MessageBase):
    def __init__(self, request_id: int, envelope_serialisation_code: Optional[str]=None, protocol_version: Optional[str]=None, 
                 payload_serialisation_code: Optional[str]=None):
        MessageBase.__init__(self, envelope_serialisation_code, protocol_version)
        self.request_id = request_id
        self.payload_serialisation_code = payload_serialisation_code


class Acknowledge(ResponseBase):
    pass

    

class Response(ResponseBase):
    def __init__(self, request_id: int, envelope_serialisation_code: str, protocol_version: str,
                 result: Any=None, 
                 payload_serialisation_code: Optional[str]=None,
                 response_type: ResponseType=ResponseType.NORMAL, acknowledge: bool=False,
                 final: Optional[FinalType]=None,   # only relevant to multipart responses
                 message_id: Optional[int]=None     # only needed if acknowledge is set
                 ):
        ResponseBase.__init__(self, request_id, envelope_serialisation_code, protocol_version,
                              payload_serialisation_code=payload_serialisation_code)
        self.result = result
        self.response_type = response_type
        self.acknowledge = acknowledge
        self.final = final
        self.message_id = message_id


class ExceptionCategory(StrEnum):
    CALLER = 'r'  # the caller made an error (invalid param value or type, not authorised, target not found)
    CALLEE = 'e'  # a callee error occurred
    TRANSPORT = 't'  # some kind of transport error, or parse error.  # FIXME: Do we need this? Does this make sense?


class ExceptionInfo:
    def __init__(self, category: ExceptionCategory, exc_type: str, msg: str, details: str = '',
                 value: Optional[int]=None):
        self.category = category
        self.exc_type = exc_type
        self.msg = msg
        self.details = details
        self.value = value


class ExceptionResponse(ResponseBase):
    def __init__(self, request_id: int, exc_info: ExceptionInfo=None, acknowledge: bool=False,
                  envelope_serialisation_code: Optional[str]=None, protocol_version: Optional[str]=None,
                 payload_serialisation_code: Optional[str] = None,
                 message_id: Optional[int]=None     # only needed if acknowledge is set
                  ):
        ResponseBase.__init__(self, request_id, envelope_serialisation_code, protocol_version,
                              payload_serialisation_code=payload_serialisation_code)
        self.exc_info = exc_info
        self.acknowledge = acknowledge
        self.message_id = message_id


# # FIXME: Should we even have these? How can they work in an event based system? Maybe for start-tls etc?
# class ControlMsg:
#     def __init__(self, msg: str='', data: bytes = b''):
#         self.msg = msg
#         self.data = data
#         self.msg.encode('ascii')  # raises an exception of not a valid ControlMsg (ie, non ascii)


class RequestCallbackInfo:
    def __init__(self, func: Callable, param_details: Optional[list[str]]=None):
        self.func = func
        self.param_details = param_details    # None = unspecified.
                                              # FIXME: just using None for now. Do we want to send types too?


class IterableCallbackInfo:
    def __init__(self, iter: Iterable, return_details: Any=None):
        self.iter = iter
        self.return_details = return_details   # FIXME: This is not done yet.
