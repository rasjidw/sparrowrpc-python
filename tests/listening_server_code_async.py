import argparse
import asyncio
import logging
import sys

from sparrowrpc import export, InvalidParams
from sparrowrpc.engine import ProtocolEngine

from sparrowrpc.asyncio import AsyncDispatcher, AsyncMsgChannel, AsyncMsgChannelInjector, AsyncCallbackProxy

import common_data



logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
                    )


@export
async def hello_world(name=None):
    if name:
        return f'Hello {name}!'
    else:
        return f'Hello world!'


@export(multipart_response=True)
async def multipart_response():
    for fruit in common_data.MULTPART_RESPONSE_ITEMS:
        yield fruit


@export
async def optional_progress_callback(progress: AsyncCallbackProxy = None):
    if progress:
        progress.set_to_notification()
    total = 0
    for x in range(10):
        total += x
        if progress:
            await progress(message=f'Added {x} to total')
    return total


@export(injectable_params=dict(channel=AsyncMsgChannelInjector))
async def iterable_param(nums, channel):
    assert isinstance(channel, AsyncMsgChannel)
    total = 0
    async for x in nums:
        total += x
    return total


@export
async def division(a, b):
    try:
        result = a / b
    except (TypeError, ValueError) as e:
        raise InvalidParams(f'Invalid param type: {str(e)}')
    except ZeroDivisionError:
        raise InvalidParams('b must not be 0')
    if a == 11:
        raise RuntimeError('a == 11 is a server bug')
    return result


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tcp_port', type=int, help='Port for TCP Server')
    parser.add_argument('--ws_port', type=int, help='Port for websocket server')
    parser.add_argument('--uds_path', help='Path for listening unix socket')
    args = parser.parse_args()

    tcp_port = args.tcp_port
    ws_port = args.ws_port
    uds_path = args.uds_path

    engine = ProtocolEngine()
    dispatcher = AsyncDispatcher(num_threads=5)
    servers = list()

    if tcp_port:
        from sparrowrpc.asyncio.transports import AsyncTcpListener
        tcp_server = AsyncTcpListener(engine, dispatcher)
        await tcp_server.run_server('127.0.0.1', tcp_port, block=False)
        servers.append(tcp_server)
    if ws_port:
        from sparrowrpc.asyncio.transports.websockets import AsyncWebsocketListener
        ws_server = AsyncWebsocketListener(engine, dispatcher)
        await ws_server.run_server('127.0.0.1', ws_port, block=False)
        servers.append(ws_server)
    if uds_path:
        from sparrowrpc.asyncio.transports import AsyncUnixSocketListener
        print(f'Listening on unix socket {uds_path}')
        uds_server = AsyncUnixSocketListener(engine, dispatcher)
        await uds_server.run_server(uds_path, block=False)
        servers.append(uds_server)

    # block on first server
    if not servers:
        raise ValueError('At least one server should be run')
    
    first_server = servers[0]
    await first_server.block()

    for server in servers[1:]:
        await server.shutdown_server()

    await dispatcher.shutdown()
    print('Server stopped.')


if __name__ == '__main__':
    asyncio.run(main())
