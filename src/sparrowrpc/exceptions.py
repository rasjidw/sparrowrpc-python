


class CallerException(Exception):
    @classmethod
    def get_subclasses(cls):
        for subclass in cls.__subclasses__():
            yield from subclass.get_subclasses()
            yield subclass


class TargetNotFound(CallerException):
    pass


class InvalidParams(CallerException):
    pass


class CalleeException(Exception):
    pass


class ProtocolError(Exception):
    pass
