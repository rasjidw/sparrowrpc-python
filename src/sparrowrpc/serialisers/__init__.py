from abc import ABC, abstractmethod
import json
import sys
from typing import Any


try:
    if sys.implementation.name == 'micropython':
        import umsgpack as msgpack  # micropython  # type: ignore
    else:
        import msgpack
    have_msgpack = True
except ImportError:
        have_msgpack = False

try:
    import cbor2
    have_cbor2 = True
except ImportError:
    have_cbor2 = False


class BaseSerialiser(ABC):
    sig = None
    @abstractmethod
    def serialise(self, obj_data: Any) -> bytes:
        raise NotImplementedError()
    @abstractmethod
    def deserialise(self, bin_data: bytes) -> Any:
        raise NotImplementedError()


class JsonSerialiser(BaseSerialiser):
    sig = 'JS'
    def serialise(self, obj_data: Any) -> bytes:
        return json.dumps(obj_data).encode()
    def deserialise(self, bin_data: bytes) -> Any:
        return json.loads(bin_data.decode())


if have_msgpack:
    class MsgpackSerialiser(BaseSerialiser):
        sig = 'MP'
        def serialise(self, obj_data: Any) -> bytes:
            return msgpack.packb(obj_data)
        def deserialise(self, bin_data: bytes) -> Any:
            return msgpack.unpackb(bin_data)


if have_cbor2:
    class CborSerialiser(BaseSerialiser):
        sig = 'CBOR'
        def serialise(self, obj_data: Any) -> bytes:
            return cbor2.dumps(obj_data)
        def deserialise(self, bin_data: bytes) -> Any:
            return cbor2.loads(bin_data)
