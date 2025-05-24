from __future__ import annotations

from sparrowrpc.registers import FunctionRegister, default_func_register


__all__ = ['ExportDecorator', 'make_export_decorator', 'export']


class ExportDecorator:
    def __init__(self, default_namespace='', func_register = None):
        self.default_namespace = default_namespace
        self.register = func_register if func_register else default_func_register
        assert isinstance(self.register, FunctionRegister)

    def __call__(self, _func=None, target_name=None, namespace=None, auth_groups=None, multipart_response=False, injectable_params=None, non_blocking=False):
        def decorate(func):
            self.register.register_func(func, target_name, namespace, auth_groups, multipart_response, injectable_params=injectable_params, non_blocking=non_blocking)
            return func
        if _func and callable(_func):
            return decorate(_func)
        else:
            return decorate


def make_export_decorator(default_namespace=''):
    return ExportDecorator(default_namespace)


export = make_export_decorator()