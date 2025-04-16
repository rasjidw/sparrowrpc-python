import json
import logging

from binarychain import BinaryChain

from lucido.core import FunctionRegister

from ..engines import ProtocolEngineBase
from ..core import OutgoingAcknowledge, OutgoingException, OutgoingLinkedMessage, OutgoingNotification, OutgoingRequest, OutgoingResponse
from ..core import IncomingAcknowledge, IncomingException, IncomingLinkedMessage, IncomingNotification, IncomingRequest, IncomingResponse
from ..core import ControlMsg, RequestBase, RequestCallbackInfo, RequestType, ResponseType, MtpeExceptionCategory, MtpeExceptionInfo, IterableCallbackInfo
from ..core import RequestType, ResponseType, FinalType, ResponseBase
from ..threaded import IterableCallbackProxy, CallbackProxy

from ..exceptions import ProtocolError


log = logging.getLogger(__name__)


class ProtocolEngine(ProtocolEngineBase):
    _sig = 'jsonrpc2l'
    max_bc_length = 1   # params or result (prefix not counted)
    def __init__(self):
        self.message_id = 1
        self.always_send_ids = False

        self._make_map = {OutgoingRequest: self._make_out_request,
                          OutgoingNotification: self._make_out_notification,
                          OutgoingResponse: self._make_out_resp,
                          OutgoingException: self._make_out_except,
                          }
        
        self.channels = list()

    def outgoing_message_to_binary_chain(self, message: RequestBase, message_id: int):
        try:
            make_method = self._make_map[type(message)]
            msg_dict = make_method(message, message_id)
        except KeyError:
            raise TypeError(f'Invalid message type {type(message)}')
        return BinaryChain(parts=[json.dumps(msg_dict).encode()])

    def _check_supported_request(self, message: RequestBase):
        if message.callback_request_id:
            raise ValueError('Callbacks not supported with this engine.')
        if message.node:
            raise ValueError('Nodes not supported with this engine.')
        if message.raw_binary:
            raise ValueError('Raw binary not supported with this engine.')
    
    def _make_method_name(self, message: RequestBase):
        if message.namespace:
            return f'{message.namespace}|{message.target}'
        else:
            return f'|{message.target}'

    def _make_out_request(self, message: OutgoingRequest, message_id: int):
        self._check_supported_request(message)
        if message_id is None:
            raise ValueError('message id is required on Outgoing Requests')
        if message.request_type != RequestType.NORMAL:
            raise ValueError('Only normal requests supported with this engine')
        if message.acknowledge:
            raise ValueError('Acknowlege not supported with this engine.')
        data = dict(jsonrpc="2.0l", method=self._make_method_name(message), id=message_id)
        if message.params:
            data['params'] = message.params
        return data
        
    def _make_out_notification(self, message: OutgoingNotification, message_id: int|None):
        self._check_supported_request(message)
        data = dict(jsonrpc="2.0l", method=self._make_method_name(message))
        if message.params:
            data['params'] = message.params
        return data
    
    def _check_supported_response(self, message: ResponseBase):
        if message.acknowledge:
            raise ValueError('Acknowlege not supported with this engine.')
        if message.final:
            raise ValueError('Multipart responses not supported with this engine.')

    def _make_out_resp(self, message: OutgoingResponse, message_id: int):
        self._check_supported_response(message)
        return dict(jsonrpc="2.0l", result=message.result, id=message.request_id)

    def _make_out_except(self, message: OutgoingException, message_id: int):
        self._check_supported_response(message)
        exc_info = message.exc_info
        except_data = {'cat': str(exc_info.category), 'type': exc_info.type}
        if exc_info.msg:
            except_data['msg'] = exc_info.msg
        if exc_info.details:
            except_data['details'] = exc_info.details
        if exc_info.value is not None:
            except_data['value'] = exc_info.value
        return dict(jsonrpc="2.0l", error=except_data, id=message.request_id)
        
    def parse_incoming_envelope(self, incoming_bin_chain: BinaryChain):
        raise RuntimeError('Not supported for jsonrcp2l')
    
    def parse_incoming_message(self, incoming_bin_chain: BinaryChain):
        msg_parts = incoming_bin_chain.parts
        if not (incoming_bin_chain.prefix == '' and len(msg_parts) == 1):
            raise ProtocolError('invalid binary chain')

        data = json.loads(msg_parts[0].decode())
        if 'method' in data:
            parts = data['method'].split('|', 1)
            if len(parts) == 1:
                namespace = ''
                target = parts[0]
            else:
                namespace, target = parts
            if 'id' in data:
                return IncomingRequest(id=data['id'],
                                       namespace=namespace,
                                       target=target,
                                       params=data.get('params'))
            else:
                return IncomingNotification(target=data['method'], params=data.get('params'))
        if 'result' in data:
            return IncomingResponse(request_id=data['id'], result=data['result'])
        if 'error' in data:
            raw_except_data = data['error']
            exc_info = MtpeExceptionInfo(MtpeExceptionCategory(raw_except_data['cat']), raw_except_data['type'], raw_except_data['msg'])
            return IncomingException(request_id=data['id'], exc_info=exc_info)
        raise RuntimeError('invalid message passed in')

    def get_system_register(self):
        register = FunctionRegister(namespace='#sys')
        register.register_func(self._ping, 'ping')
        register.register_func(self.get_engine_signature, 'get_sig')
        return register
    
    def get_engine_signature(self):
        return f'{self._sig}'.lower()
    
    def _ping(self):
        return 'pong'
