class StrEnum(str):
    _valid_values = set()

    def __new__(cls, value):
        if value not in cls.get_valid_values():
            raise ValueError(f'{value!r} is not a valid {cls.__name__}')
        member = str(value)
        return member
    
    @classmethod
    def get_valid_values(cls):
        if cls._valid_values:
            return cls._valid_values
        
        valid_values = set()
        for key, key_value in cls.__dict__.items():
            if key[0] == '_':
                continue
            if not isinstance(key_value, str):
                continue
            valid_values.add(key_value)
        cls._valid_values = valid_values
        return valid_values
