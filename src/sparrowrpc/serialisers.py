from abc import ABC, abstractmethod
import json
import sys
from typing import Any


try:
    if sys.implementation.name == 'micropython':
        import umsgpack as msgpack  # micropython  # type: ignore
    else:
        import msgpack
except ImportError:
        msgpack = None

try:
    import cbor2
except ImportError:
    cbor2 = None


class BaseSerialiser(ABC):
    sig = None
    @abstractmethod
    def serialise(self, obj_data: Any) -> bytes:
        raise NotImplementedError()
    @abstractmethod
    def deserialise(self, bin_data: bytes) -> Any:
        raise NotImplementedError()


class JsonSerialiser(BaseSerialiser):
    sig = 'j'
    def serialise(self, obj_data: Any) -> bytes:
        return json.dumps(obj_data).encode()
    def deserialise(self, bin_data: bytes) -> Any:
        return json.loads(bin_data.decode())


if msgpack:
    class MsgpackSerialiser(BaseSerialiser):
        sig = 'm'
        # using dumps and loads for micropython compatibility
        def serialise(self, obj_data: Any) -> bytes:
            return msgpack.dumps(obj_data) # type: ignore
        def deserialise(self, bin_data: bytes) -> Any:
            return msgpack.loads(bin_data)
else:
    MsgpackSerialiser = None


if cbor2:
    class CborSerialiser(BaseSerialiser):
        sig = 'c'
        def serialise(self, obj_data: Any) -> bytes:
            return cbor2.dumps(obj_data)
        def deserialise(self, bin_data: bytes) -> Any:
            return cbor2.loads(bin_data)
else:
    CborSerialiser = None
