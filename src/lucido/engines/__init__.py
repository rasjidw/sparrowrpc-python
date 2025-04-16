from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
import logging
import sys
from typing import Any


from binarychain import BinaryChain

from ..core import RequestBase


log = logging.getLogger(__name__)


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
