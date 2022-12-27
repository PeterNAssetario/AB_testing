from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Dict, Optional, Sequence, Type, TypeVar

T = TypeVar("T")
Formatter = Callable[[Any], str]

_DEFAULT_DATA_FORMATTERS: Dict[Type[Any], Formatter] = {}


def _get_data_formatter(v: Any) -> Formatter:
    formatter = _DEFAULT_DATA_FORMATTERS.get(type(v))
    if formatter is None:
        raise TypeError(f"{type(v)} has no defined formatter.")
    return formatter


def _escape_presto(val: str) -> str:
    escaped = val.replace("'", "''")
    return f"'{escaped}'"


def _format_none(_v: None) -> str:
    return "NULL"


def _format_default(v: T) -> str:
    return str(v)


def _format_date(v: date) -> str:
    return f"DATE '{v:%Y-%m-%d}'"


def _format_datetime(v: datetime) -> str:
    return f"""TIMESTAMP '{v.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}'"""


def _format_bool(v: bool) -> str:
    return str(v)


def _format_str(v: str) -> str:
    return _escape_presto(v)


def _format_decimal(v: Decimal) -> str:
    escaped = _escape_presto(f"{v:f}")
    return f"DECIMAL {escaped}"


def _format_seq(v: Sequence[T]) -> str:
    results = []
    for item in v:
        formatter = _get_data_formatter(item)

        formatted = formatter(item)
        if not isinstance(
            formatted,
            (str,),
        ):
            # force string format
            if isinstance(
                formatted,
                (
                    float,
                    Decimal,
                ),
            ):
                formatted = f"{formatted:f}"
            else:
                formatted = f"{formatted}"
        results.append(formatted)
    return f"""({", ".join(results)})"""


_DEFAULT_DATA_FORMATTERS = {
    type(None): _format_none,
    date: _format_date,
    datetime: _format_datetime,
    int: _format_default,
    float: _format_default,
    Decimal: _format_decimal,
    bool: _format_bool,
    str: _format_str,
    list: _format_seq,
    set: _format_seq,
    tuple: _format_seq,
}


def _format_any_value(v: Any) -> str:
    formatter = _get_data_formatter(v)
    return formatter(v)


def apply_sql_parameters(sql: str, parameters: Optional[Dict[str, Any]]) -> str:
    if parameters is None:
        return sql

    format_parameters = {k: _format_any_value(v) for k, v in parameters.items()}

    return sql % format_parameters
