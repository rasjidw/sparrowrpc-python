from __future__ import annotations


from typing import Any, Iterable, Optional, Callable
import sys

if sys.version_info >= (3, 11) or sys.implementation.name == 'micropython':
    from enum import StrEnum
else:
    from backports.strenum import StrEnum # type: ignore




__all__ = ['RequestType', 'ResponseType', 'FinalType', 'PushIterableInfo', 'RequestBase', 'OutgoingRequest', 
           'OutgoingNotification', 'ResponseBase', 'OutgoingResponse', 'MessageSentEvent', 'AcknowledgeBase',
           'OutgoingAcknowledge', 'IncomingAcknowledge', 'MtpeExceptionCategory', 'MtpeExceptionInfo', 
           'OutgoingException', 'IncomingRequest', 'IncomingNotification', 'IncomingResponse', 'IncomingException',
           'ControlMsg', 'RequestCallbackInfo', 'IterableCallbackInfo', ]


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


class PushIterableInfo:
    def __init__(self, iter: Optional[Iterable] = None, return_details: Any = None):
        self.iter = iter
        self.return_details = return_details     # FIXME: return_details is not done yet.


class EventBase:
    @property
    def _properties(self):
        return self.__dict__.items()
    
    def __repr__(self):
        items = (f"{k}={v!r}" for k, v in self._properties if not k.startswith('_'))
        return f"<{self.__class__.__name__}: {', '.join(items)}>"



class RequestBase(EventBase):
    def __init__(self, target: str, namespace: str='', node: Optional[str]=None, params: Optional[dict]=None,  # param name -> value
                 callback_request_id: Optional[int]=None, raw_binary: bool=False, data: Optional[bytearray]=None  # only set / valid if raw_binary = True
                 ):
        self.target = target
        self.namespace = '' if namespace is None else namespace
        self.node = node
        self.params = params
        self.callback_request_id = callback_request_id
        self.raw_binary = raw_binary
        self.data = data


class OutgoingRequest(RequestBase):
    def __init__(self, target: str, namespace: str='', node: Optional[str]=None, params: Optional[dict]=None,  # param name -> value
                 callback_request_id: Optional[int]=None, raw_binary: bool=False, data: Optional[bytearray]=None, 
                 acknowledge: bool=False, request_type: RequestType=RequestType.NORMAL, 
                 callback_params: Optional[dict]=None # param name -> CallbackInfo
                 ):
        RequestBase.__init__(self, target, namespace, node, params, callback_request_id, raw_binary, data)
        self.acknowledge = acknowledge
        self.request_type = request_type
        self.callback_params = callback_params

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


class ResponseBase(EventBase):
    def __init__(self, request_id: int, response_type: ResponseType=ResponseType.NORMAL, acknowledge: bool=False,
                 raw_binary: bool=False, final: Optional[FinalType]=None   # only relevent to multipart responses
                 ):
        self.request_id = request_id
        self.response_type = response_type
        self.acknowledge = acknowledge
        self.raw_binary = raw_binary
        self.final = final


class OutgoingResponse(ResponseBase):
    def __init__(self, request_id: int, result: Any=None, response_type: ResponseType=ResponseType.NORMAL, acknowledge: bool=False,
                 raw_binary: bool=False, final: Optional[FinalType]=None    # only relevent to multipart responses
                 ):
        ResponseBase.__init__(self, request_id, response_type, acknowledge, raw_binary, final)
        self.result = result


class MessageSentEvent(EventBase):
    def __init__(self, request_id: int):
        self.request_id = request_id


class AcknowledgeBase(EventBase):
    def __init__(self, request_id: int):
        self.request_id = request_id


class OutgoingAcknowledge(AcknowledgeBase):
    pass


class IncomingAcknowledge(AcknowledgeBase):
    pass


class MtpeExceptionCategory(StrEnum):
    CALLER = 'r'  # the caller made an error (invalid param value or type, not authorised, target not found)
    CALLEE = 'e'  # a callee error occured
    TRANSPORT = 't'  # some kind of transport error, or parse error.  # FIXME: Do we need this? Does this make sense?


class MtpeExceptionInfo:
    def __init__(self, category: MtpeExceptionCategory, type: str, msg: str, details: str = '', value: Optional[int]=None):
        self.category = category
        self.type = type
        self.msg = msg
        self.details = details
        self.value = value


class OutgoingException(ResponseBase):
    def __init__(self, request_id: int, exc_info: MtpeExceptionInfo):
        ResponseBase.__init__(self, request_id)
        self.exc_info = exc_info


class IncomingRequest(OutgoingRequest):
    def __init__(self, id: int, target: str, namespace: str='', node: Optional[str]=None, params: Optional[dict]=None,  # param name -> value
                 callback_request_id: Optional[int]=None, raw_binary: bool=False, data: Optional[bytearray]=None, 
                 acknowledge: bool=False, request_type: RequestType=RequestType.NORMAL, 
                 callback_params: Optional[dict]=None # param name -> CallbackInfo
                 ):
        OutgoingRequest.__init__(self, target, namespace, node, params, callback_request_id, raw_binary, data, 
                 acknowledge, request_type, callback_params)
        self.id = id


class IncomingNotification(OutgoingNotification):
    def __init__(self, target: str, namespace: str='', node: Optional[str]=None, params: Optional[dict]=None,  # param name -> value
                 callback_request_id: Optional[int]=None, raw_binary: bool=False, data: Optional[bytearray]=None,  # only set / valid if raw_binary = True
                 id: Optional[int]=None
                 ):
        OutgoingNotification.__init__(self, target, namespace, node, params, callback_request_id, raw_binary, data)
        self.id = id  # FIXME: Should notifications even store an id. Maybe rename to nofication_id


class IncomingResponse(ResponseBase):
    def __init__(self, request_id: int, response_type: ResponseType=ResponseType.NORMAL, acknowledge: bool=False,
                 raw_binary: bool=False, final: Optional[FinalType]=None, result: Any=None, id: Optional[int]=None):
        ResponseBase.__init__(self, request_id, response_type, acknowledge, raw_binary, final)
        self.result = result
        self.id = id


class IncomingException(ResponseBase):
    def __init__(self, request_id: int, exc_info: Optional[MtpeExceptionInfo]=None, acknowledge: bool=False, id: Optional[int]=None):
        ResponseBase.__init__(self, request_id, acknowledge=acknowledge)
        self.exc_info = exc_info
        self.id = id


# FIXME: Should we even have these? How can they work in an event based system? Maybe for start-tls etc?
class ControlMsg:
    def __init__(self, msg: str='', data: bytes = b''):
        self.msg = msg
        self.data = data
        self.msg.encode('ascii')  # raises an exception of not a valid ControMsg (ie, non ascii)


class RequestCallbackInfo:
    def __init__(self, func: Callable, param_details: Optional[list[str]]=None):
        self.func = func
        self.param_details = param_details    # None = unspecified. FIXME: just using None for now. Do we want to send types too? 


class IterableCallbackInfo:
    def __init__(self, iter: Iterable, return_details: Any=None):
        self.iter = iter
        self.return_details = return_details   # FIXME: This is not done yet.
