from __future__ import annotations

from typing import Dict, List, Tuple

from binarychain import BinaryChain

from .exceptions import ProtocolError
from .encoders.base import EncoderDecoderBase
from .serialisers import BaseSerialiser
from . import messages, serialisers
from .encoders import hs, v050
from .registers import FunctionRegister


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
        envelope_serialisation_code = incoming_bin_chain.prefix
        msg_parts = incoming_bin_chain.parts

        try:
            envelope_serialiser = self.serialisers[envelope_serialisation_code]
        except KeyError:
            raise ProtocolError(f'Serialiser {envelope_serialisation_code} not found')
        
        if not msg_parts:
            raise ProtocolError('No envelope data found - empty binary chain') 
        
        raw_envelope_data, raw_payload_data = msg_parts[0], msg_parts[1:]
        envelope_data = envelope_serialiser.deserialise(raw_envelope_data)

        # print(envelope_data)
        # print(type(envelope_data))

        if not isinstance(envelope_data, dict):
            raise ProtocolError('Envelope must be a map / dict')
        
        try:
            protocol_version = envelope_data.pop('v')
        except KeyError:
            raise ProtocolError('No protocol version not found')
        
        try:
            engine = self.encoders[protocol_version]
        except KeyError:
            raise ProtocolError(f'Engine with tag {protocol_version} not found')
        
        # decode the envelope
        message = engine.decode_raw_envelope(envelope_data, envelope_serialisation_code)

        # parse the payload data
        engine.parse_payload_data(message, raw_payload_data)

        return message        

    def message_to_binary_chain(self, message: messages.MessageBase):
        envelope_serialisation_code = message.envelope_serialisation_code
        if not envelope_serialisation_code:
            raise ValueError('Envelope Serialisation Code must be set')
        
        try:
            envelope_serialiser = self.serialisers[envelope_serialisation_code]
        except KeyError:
            raise ValueError(f'Serialiser {envelope_serialisation_code} not found')

        protocol_version = message.protocol_version
        if not protocol_version:
            raise ValueError('Protocol version must be set')
        
        try:
            engine = self.encoders[protocol_version]
        except KeyError:
            raise ProtocolError(f'Engine with tag {protocol_version} not found')

        envelope_data = engine.encode_envelope(message)
        msg_parts = [envelope_serialiser.serialise(envelope_data)]
        msg_parts.extend(engine.serialise_payload_data(message))

        return BinaryChain(envelope_serialisation_code, msg_parts)
    
    def get_system_register(self):
        register = FunctionRegister(namespace='#sys')
        register.register_func(self._ping, 'ping')
        return register

    @staticmethod
    def _ping():
        return 'pong'
