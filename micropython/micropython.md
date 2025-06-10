# Notes for running under Micropython

Currently only tested under the Linux port of Micropython, and on Micropython under Pyscript.

## Installation

```
micropython -m mip install github:rasjidw/sparrowrpc-python
micropython -m mip install github:peterhinch/micropython-msgpack  # optional
micropython -m mip install cbor2  # optional
```

**NOTE:** Sparrowrpc installs an updated version of the abc standard module. It is recommended to remove any existing abc.mpy file before installing sparrowrpc.
