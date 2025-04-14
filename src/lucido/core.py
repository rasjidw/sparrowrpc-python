from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import inspect
import logging



log = logging.getLogger(__name__)




@dataclass
class FuncInfo:
    target_name: str
    namespace: str
    auth_groups: list[str]  # FIXME: maybe something more general, like tags.
    multipart_request: str   # argument name to use as the incoming iterator / generator  # FIXME: Probably remove this.
    multipart_reponse: bool
    func: callable
    is_iterable_callback: bool = False   # currently only used for iterable callbacks - not sure if it makes sense elsewhere
    injectable_params: dict = None       # param name to callable that returns the injected param.




class FunctionRegister:
    def __init__(self, namespace: str = None):
        self.namespace = namespace if namespace else ''
        self._register = dict()   # dict[namespace][target_name] -> FuncInfo
    def register_func(self, func, target_name=None, namespace=None, auth_groups=None, multipart_request=None, multipart_response=False, injectable_params=None):
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
            target_name = func_data['__qualname__']
        if auth_groups is None:
            auth_groups = []
        if namespace not in self._register:
            self._register[namespace] = dict()
        func_info = FuncInfo(target_name, namespace, auth_groups, multipart_request, multipart_response, func, injectable_params=injectable_params)
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

# decorators

def make_export_decorator(defaul_namespace=None):
    return ExportDecorator(defaul_namespace)


class ExportDecorator:
    def __init__(self, default_namespace='', func_register = None):
        self.default_namespace = default_namespace
        self.register = func_register if func_register else default_func_register
        assert isinstance(self.register, FunctionRegister)
    def __call__(self, _func=None, target_name=None, namespace=None, auth_groups=None, multipart_request=None, multipart_response=False, injectable_params=None):
        def decorate(func):
            self.register.register_func(func, target_name, namespace, auth_groups, multipart_request, multipart_response, injectable_params=injectable_params)
            return func
        if _func and callable(_func):
            return decorate(_func)
        else:
            return decorate
        

