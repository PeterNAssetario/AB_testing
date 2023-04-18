import sys
import warnings
from enum import Enum
from typing import Any, Callable, Type, TypeVar, Union

from pydantic import BaseModel

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

T = TypeVar("T")
TEnum = TypeVar("TEnum", bound=Enum)

FallbackEnum = Union[TEnum, Literal["unknown"]]
FallbackUnion = Union[T, Literal["unknown"]]


def pydantic_fallback_enum_validator(
    enum_cls: Type[TEnum],
) -> Callable[[Type[BaseModel], Any], FallbackEnum[TEnum]]:
    def validator(cls: Type[BaseModel], v: Any) -> FallbackEnum[TEnum]:
        try:
            return enum_cls(v)
        except ValueError:
            warnings.warn(
                f"Unknown value {v} for {enum_cls.__name__}, try updating ml-lib to the latest version"
            )
            return "unknown"

    return validator
