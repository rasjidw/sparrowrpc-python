# Notes for running under Micropython

Currently only tested under the Linux port of Micropython, and on Micropython under Pyscript.

## Std Modules to install with mip

__future__
collections
collections-defaultdict
datetime
inspect
logging
traceback

## mip installable externally available module

micropython -m mip install github:josverl/micropython-stubs/mip/typing.py

## Other modules externally available

umsgpack - from https://github.com/peterhinch/micropython-msgpack
udataclasses - from https://github.com/dhrosa/udataclasses

## Other modules / stubs in this repo

abc
enum
uasync
