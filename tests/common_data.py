from collections import namedtuple
import sys
from tempfile import NamedTemporaryFile


MULTIPART_RESPONSE_ITEMS = ['apple', 'banana', 'mango']

on_win32 = sys.platform == 'win32'
port_names = ['threaded_tcp', 'threaded_ws', 'threaded_uds', 'async_tcp', 'async_ws', 'async_uds']
connect_list = port_names.copy()
win32_connect_list = ['threaded_tcp', 'threaded_ws', 'async_tcp', 'async_ws']
Ports = namedtuple('Ports', port_names)


def get_ports(start_port):
    threaded_socket_path = async_socket_path = None
    if sys.platform != 'win32':
        with NamedTemporaryFile(suffix='.sock') as ft:
            threaded_socket_path = ft.name
            with NamedTemporaryFile(suffix='.sock') as fa:
                async_socket_path = fa.name
    return Ports(start_port, start_port + 1, threaded_socket_path, start_port + 10, start_port + 11, async_socket_path)
