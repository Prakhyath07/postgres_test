from enum import Enum


class ProductType(Enum):

    INDEX = "INDEX"
    EQUITY = "EQUITY"
    NFO = "NFO"

    @classmethod
    def choices(cls):
        return tuple((i.name, i.value) for i in cls)
