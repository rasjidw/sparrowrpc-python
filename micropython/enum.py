class StrEnum(str):
    def __new__(cls, value):
        member = str(value)
        return member
    
