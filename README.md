# Sparrow RPC

Sparrow PRC is a language independent, lightweight Event Based Multi-Transport Peer-to-Peer bidirectional RPC System and Protocol Engine.

The purpose of Sparrow is to not just be a rpc system, but to also be a high level api for the creation of
sophisticated protocols without worrying about 'bytes on the wire'.

## Core Features

* Transport Agnostic including TCP sockets, Websockets, Stdin/Stdout, Named Pipes (TO DO), Unix sockets (TO DO)
* Serialisaiton Agnostic. Supports Json, MsgPack and CBOR (TO DO).
* Fully Peer-to-Peer - both sides of any connection are equal.
* RPC requests can flow both directions, and can be interleaved with each other.
* RPC calls support multipart responses (ie, can return an iterable).
* RPC calls can include remote callbacks.
* Pub / Sub style of calls is also supported.
* Low network overhead when paired with a compact serialisation library like MsgPack or CBOR.
* Efficient binary data transfer even when using a serialisation library that does not support binary data.


## Status

This is in heavy development and is currently pre-alpha.

