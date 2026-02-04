from __future__ import annotations

from typing import Dict, List, Tuple

from binarychain import BinaryChain

from .exceptions import ProtocolError
from .encoders.base import EncoderDecoderBase
from .serialisers import BaseSerialiser
from . import messages, serialisers
from .encoders import hs, v050
from .registers import FunctionRegister

# FIXME: Does the sig_override even work?

class ProtocolEngine:
    def __init__(self, load_default_serialisers=True, load_default_encoders=True,
                 always_send_ids=False) -> None:
        self.serialisers: Dict[str, BaseSerialiser] = dict()
        self.encoders: Dict[str, EncoderDecoderBase] = dict()
        self.always_send_ids = always_send_ids

        if load_default_serialisers:
            self.load_default_serialisers()

        if load_default_encoders:
            self.load_default_encoders()

    def load_default_serialisers(self):
        self.add_serialiser(serialisers.JsonSerialiser())

        if serialisers.msgpack:
            self.add_serialiser(serialisers.MsgpackSerialiser())

        if serialisers.cbor2:
            self.add_serialiser(serialisers.CborSerialiser())

    def add_serialiser(self, serialiser: BaseSerialiser, sig_override: str=''):
        sig = sig_override if sig_override else serialiser.sig
        if sig in self.serialisers.keys():
            raise ValueError(f'Serialiser with sig {sig} already exists')
        if sig is None:
            raise ValueError(f'Sig for serialiser {serialiser} not set')
        self.serialisers[sig] = serialiser

    def load_default_encoders(self):
        self.load_encoder(hs.BasicEncoderDecoder(self))
        self.load_encoder(v050.V050EncoderDecoder(self))

    def load_encoder(self, engine: EncoderDecoderBase):
        self.encoders[engine.TAG] = engine

    def parse_incoming_message(self, incoming_bin_chain: BinaryChain):
        protocol_version = incoming_bin_chain.parts[0].decode()

        try:
            encoder = self.encoders[protocol_version]
        except KeyError:
            raise ProtocolError(f'Engine with tag {protocol_version} not found')

        return encoder.binary_list_to_incoming_message(incoming_bin_chain.parts[1:])

    def message_to_binary_chain(self, message: messages.MessageBase):
        protocol_version = message.protocol_version
        if not protocol_version:
            raise ValueError('Protocol version must be set')
        
        try:
            encoder = self.encoders[protocol_version]
        except KeyError:
            raise ProtocolError(f'Engine with tag {protocol_version} not found')

        binary_list = encoder.outgoing_message_to_binary_list(message)
        parts = [protocol_version.encode()] + binary_list
        return BinaryChain(parts=parts)

    def get_system_register(self):
        register = FunctionRegister(namespace='#sys')
        register.register_func(self._ping, 'ping')
        return register

    @staticmethod
    def _ping():
        return 'pong'
