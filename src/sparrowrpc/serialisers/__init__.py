from abc import ABC, abstractmethod
import json
from typing import Any


try:
    import msgpack
    have_msgpack = True
except ImportError:
    try:
        import umsgpack as msgpack  # micropython
        have_msgpack = True
    except ImportError:
        have_msgpack = False



class BaseSerialiser(ABC):
    sig = None
    @abstractmethod
    def serialise(self, obj_data: Any) -> bytes:
        raise NotImplementedError()
    @abstractmethod
    def deserialise(self, bin_data: bytes) -> Any:
        raise NotImplementedError()


if have_msgpack:
    class MsgpackSerialiser(BaseSerialiser):
        sig = 'MP'
        def serialise(self, obj_data: Any) -> bytes:
            return msgpack.packb(obj_data)
        def deserialise(self, bin_data: bytes) -> Any:
            return msgpack.unpackb(bin_data)


class JsonSerialiser(BaseSerialiser):
    sig = 'JS'
    def serialise(self, obj_data: Any) -> bytes:
        return json.dumps(obj_data).encode()
    def deserialise(self, bin_data: bytes) -> Any:
        return json.loads(bin_data.decode())

