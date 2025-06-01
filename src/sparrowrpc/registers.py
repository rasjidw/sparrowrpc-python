from __future__ import annotations

from collections import defaultdict
import inspect
import logging
from typing import Iterable

try:
    from dataclasses import dataclass
except ImportError:
    from udataclasses import dataclass  # type: ignore (for micropython)


__all__ = ['FuncInfo', 'FunctionRegister', 'default_func_register', 'MsgChannelRegister', 'global_channel_register']


log = logging.getLogger(__name__)


@dataclass
class FuncInfo:
    target_name: str = ''
    namespace: str = ''
    auth_groups: list[str] = None  # FIXME: maybe something more general, like tags.
    multipart_response: bool = False
    func: callable = None
    non_blocking: bool = False  # set to true for non-async functions that are exported but can be used directly in async code as they don't block
    iterable_callback: Iterable = None   # only one of func or iterable_callback should be set
    injectable_params: dict = None       # param name to callable that returns the injected param.


class FunctionRegister:
    def __init__(self, namespace: str = None):
        self.namespace = namespace if namespace else ''
        self._register = dict()   # dict[namespace][target_name] -> FuncInfo
        
    def register_func(self, func, target_name=None, namespace=None, auth_groups=None, multipart_response=False, injectable_params=None, non_blocking=False):
        namespace = '' if namespace is None else namespace
        if not isinstance(namespace, str):
            raise TypeError('namespace must be a string')
        if self.namespace:
            if namespace:
                raise ValueError('namespace pre-set for this register')
            namespace = self.namespace
        else:
            if namespace == '#sys':
                raise ValueError(f'{namespace} is reserved and cannot be used in this register')
        if not target_name:
            func_data = dict(inspect.getmembers(func))
            target_name = func_data['__name__']
        if auth_groups is None:
            auth_groups = []
        if namespace not in self._register:
            self._register[namespace] = dict()
        func_info = FuncInfo(target_name=target_name, namespace=namespace, auth_groups=auth_groups, multipart_response=multipart_response, func=func,
                             injectable_params=injectable_params, non_blocking=non_blocking)
        if target_name in self._register[namespace]:
            raise ValueError(f'duplicate registration of {target_name} into "{namespace}" namespace')
        self._register[namespace][target_name] = func_info
        log.debug(f'Registered {func_info}')

    def get_method_info(self, target_name, namespace=None) -> FuncInfo:
        try:
            return self._register[namespace][target_name]
        except KeyError:
            return None


default_func_register = FunctionRegister()


class MsgChannelRegister:
    def __init__(self):
        self.channel_register = defaultdict(set)  # tag -> set of MsgChannels

    def register(self, msg_channel: MsgChannelBase):
        self.channel_register[msg_channel.tag].add(msg_channel)

    def unregister(self, msg_channel: MsgChannelBase):
        self.channel_register[msg_channel.tag].remove(msg_channel)

    def get_channels_by_tag(self, tag):
        return frozenset(self.channel_register[tag])


global_channel_register = MsgChannelRegister()


# typing imports at the end
from sparrowrpc.bases import MsgChannelBase
