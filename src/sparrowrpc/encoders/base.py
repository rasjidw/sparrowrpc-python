from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Any

from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from ..messages import MessageBase, ExceptionResponse
    from ..engine import ProtocolEngine


class EncoderDecoderBase(ABC):
    TAG = ''

    def __init__(self, protocol_engine: ProtocolEngine):
        self.protocol_engine = protocol_engine

    @abstractmethod
    def outgoing_message_to_binary_list(self, message: MessageBase) -> List[bytes|bytearray]:
        raise NotImplementedError()

    @abstractmethod
    def binary_list_to_incoming_message(self, binary_list: List[bytes|bytearray]) -> MessageBase:
        raise NotImplementedError()
    
    # @abstractmethod
    # def encode_envelope(self, message: MessageBase) -> Dict[str, Any]:
    #     raise NotImplementedError()
    #
    # @abstractmethod
    # def serialise_payload_data(self, message: MessageBase) -> list:
    #     raise NotImplementedError()
    #
    # @abstractmethod
    # def decode_raw_envelope(self, raw_envelope_data: dict, envelope_serialisation_code: str) -> MessageBase:
    #     raise NotImplementedError()
    #
    # @abstractmethod
    # def parse_payload_data(self, message: MessageBase, raw_payload_data: list):
    #     raise NotImplementedError()

def make_out_except_data(message: ExceptionResponse):
    exc_info = message.exc_info
    except_data: Dict[str, Any] = {'cat': str(exc_info.category), 'type': exc_info.exc_type}
    if exc_info.msg:
        except_data['msg'] = exc_info.msg
    if exc_info.details:
        except_data['details'] = exc_info.details
    if exc_info.value is not None:
        except_data['value'] = exc_info.value
    return except_data
