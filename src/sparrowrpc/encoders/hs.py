from __future__ import annotations

import logging
from typing import Dict, Any, List


from ..messages import Notification, CallBase, Request, Response, ExceptionCategory, ExceptionInfo, RequestType, ResponseType, MessageBase

from .base import EncoderDecoderBase, make_out_except_data
from ..messages import ExceptionResponse
from ..exceptions import ProtocolError

"""
This is the 'Basic' engine - basically a modified version of jsonrpc2.
The majority of the Sparrow RPC features are NOT supported with this engine.

It is expected that this engine will remain constant over time, even as the main protocol evolves and is updated.
This provides a way to perform a handshake between systems prior to knowing what (if any) common core protocol versions
they support.
"""

log = logging.getLogger(__name__)


class BasicEncoderDecoder(EncoderDecoderBase):
    TAG = 'b'
    max_bc_length = 1   # params or result (prefix not counted)

    def __init__(self, protocol_engine):
        EncoderDecoderBase.__init__(self, protocol_engine)

        self._make_map = {Request: self._make_out_request,
                          Notification: self._make_out_notification,
                          Response: self._make_out_resp,
                          ExceptionResponse: self._make_out_except,
                          }
        
        self.channels = list()

    def outgoing_message_to_binary_list(self, message: MessageBase) -> List[bytes|bytearray]:
        # if we move the payload to its own binary part, add this here
        envelope_serialiser = self.protocol_engine.serialisers[message.envelope_serialisation_code]
        return [message.envelope_serialisation_code.encode(),
                envelope_serialiser.serialise(self.encode_envelope(message))]

    def encode_envelope(self, message: MessageBase):
        try:
            # noinspection PyTypeChecker
            make_method = self._make_map[type(message)]
            return make_method(message)
        except KeyError:
            raise TypeError(f'Invalid message type {type(message)}')
        
    def serialise_payload_data(self, message: CallBase):
        return []

    @staticmethod
    def _check_supported_call(message: CallBase):
        if message.callback_request_id:
            raise ValueError('Callbacks not supported with this engine.')
        if message.payload_serialisation_code and message.payload_serialisation_code != message.envelope_serialisation_code:
            raise ValueError('Different payload serialisation not supported with this engine.')
    
    def _make_out_request(self, message: Request):
        self._check_supported_call(message)
        if message.message_id is None:
            raise ValueError('message id is required on Outgoing Requests')
        if message.request_type != RequestType.NORMAL:
            raise ValueError('Only normal requests supported with this engine')
        if message.acknowledge:
            raise ValueError('Acknowledge not supported with this engine.')
        data = self._make_out_call(message)
        data['id'] = message.message_id
        return data

    def _make_out_call(self, message: CallBase):
        data: Dict[str, Any] = dict(target=message.target, v=self.TAG)
        if message.namespace:
            data['namespace'] = message.namespace
        if message.node:
            data['node'] = message.node
        if message.params:
            data['params'] = message.params
        return data
        
    def _make_out_notification(self, message: Notification):
        self._check_supported_call(message)
        return self._make_out_call(message)

    @staticmethod
    def _check_supported_response(message: Response):
        if message.acknowledge:
            raise ValueError('Acknowledge not supported with this engine.')
        if message.response_type != ResponseType.NORMAL:
            raise ValueError('Multipart responses not supported with this engine.')

    def _make_out_resp(self, message: Response):
        self._check_supported_response(message)
        return dict(result=message.result, id=message.request_id, v=self.TAG)

    def _make_out_except(self, message: ExceptionResponse):
        except_data = make_out_except_data(message)
        return dict(error=except_data, id=message.request_id, v=self.TAG)

    def binary_list_to_incoming_message(self, binary_list: List[bytes|bytearray]) -> MessageBase:
        envelope_serialisation_code = binary_list[0].decode()
        serialiser = self.protocol_engine.serialisers[envelope_serialisation_code]
        raw_envelope_data = serialiser.deserialise(binary_list[1])
        return self.decode_raw_envelope(raw_envelope_data, envelope_serialisation_code)

    def decode_raw_envelope(self, raw_envelope_data: dict, envelope_serialisation_code: str):
        if not isinstance(raw_envelope_data, dict):
            raise ProtocolError('message must be a map/dict')

        # FIXME: We need to type-check incoming data
        # eg, what if namespace is a number etc?

        if 'target' in raw_envelope_data:
            # either a Request or Notification
            target = raw_envelope_data['target']
            namespace = raw_envelope_data.get('namespace', '')
            node = raw_envelope_data.get('node')
            params = raw_envelope_data.get('params')
            if 'id' in raw_envelope_data:
                return Request(target=target, namespace=namespace, node=node, params=params, message_id=raw_envelope_data['id'],
                               envelope_serialisation_code=envelope_serialisation_code, protocol_version=self.TAG)
            else:
                return Notification(target=target, namespace=namespace, node=node, params=raw_envelope_data.get('params'), 
                                    envelope_serialisation_code=envelope_serialisation_code, protocol_version=self.TAG)
        if 'result' in raw_envelope_data:
            return Response(request_id=raw_envelope_data['id'], result=raw_envelope_data['result'],
                            envelope_serialisation_code=envelope_serialisation_code,
                            protocol_version=self.TAG)
        if 'error' in raw_envelope_data:
            raw_except_data = raw_envelope_data['error']
            exc_info = ExceptionInfo(ExceptionCategory(raw_except_data['cat']), raw_except_data['type'], raw_except_data['msg'])
            return ExceptionResponse(request_id=raw_envelope_data['id'], exc_info=exc_info)
        raise RuntimeError('invalid message passed in')
    
    def parse_payload_data(self, message: MessageBase, raw_payload_data: list):
        if raw_payload_data:
            raise ProtocolError('payload data not supported by this protocol version')
        return
