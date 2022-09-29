import os
import sys
from typing import Any, Dict, Optional, Union

from statsd import StatsClient
from statsd.client.base import StatsClientBase, Timer

PREFIX = os.environ.get("METRICS_PREFIX")
STATSD_HOST = os.environ.get("METRICS_STATSD_HOST")
STATSD_PORT = os.environ.get("METRICS_STATSD_PORT")


class DummyStatsClient(StatsClientBase):
    def __init__(self, prefix: str):
        super().__init__()
        self._prefix = prefix

    def _send(self, data: Any) -> None:
        print("DummyStatsClient._send", data, file=sys.stderr)


_client: Optional[Union[StatsClient, DummyStatsClient]] = None
_tags: Optional[Dict[str, str]] = None


def configure_metrics(
    prefix: Optional[str] = None, tags: Optional[Dict[str, str]] = None
) -> None:
    global _client, _tags

    final_prefix = prefix = prefix or PREFIX or "unknown"

    try:
        if STATSD_HOST is not None and STATSD_PORT is not None:
            _client = StatsClient(
                host=STATSD_HOST, port=STATSD_PORT, prefix=final_prefix
            )
            _tags = {**(tags if tags is not None else {})}
        else:
            print(
                "Missing STATSD_HOST and/or STATSD_PORT environment variables",
                file=sys.stderr,
            )
            _client = DummyStatsClient(prefix=final_prefix)
            _tags = tags or {}
    except Exception as error:
        print("Failed to create StatsClient", error, file=sys.stderr)
        _client = DummyStatsClient(prefix=final_prefix)
        _tags = tags


def _stats() -> StatsClientBase:
    global _client
    if _client is None:
        configure_metrics()
    return _client


def _metric_name(stat: str, tags: Optional[Dict[str, str]] = None) -> str:
    global _tags

    final_tags = {
        **(tags if tags is not None else {}),
        **(_tags if _tags is not None else {}),
    }

    return ",".join([stat, *[f"{key}={value}" for key, value in final_tags.items()]])


def timer(stat: str, tags: Optional[Dict[str, str]] = None) -> Timer:
    return _stats().timer(_metric_name(stat, tags))


def timing(stat: str, delta: int, tags: Optional[Dict[str, str]] = None) -> None:
    return _stats().timing(_metric_name(stat, tags), delta)


def incr(stat: str, count: int, tags: Optional[Dict[str, str]] = None) -> None:
    return _stats().incr(_metric_name(stat, tags), count)


def decr(stat: str, count: int, tags: Optional[Dict[str, str]] = None) -> None:
    return _stats().decr(_metric_name(stat, tags), count)


def gauge(stat: str, count: int, tags: Optional[Dict[str, str]] = None) -> None:
    return _stats().gauge(_metric_name(stat, tags), count)
