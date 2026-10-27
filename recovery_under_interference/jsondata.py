"""Bounded, strict JSON values shared by durable state and public inputs."""

import json
import math
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any

MAX_DOCUMENT_BYTES = 16 * 1024 * 1024
MAX_DEPTH = 64
MAX_INTEGER_BITS = 4096


def plain(value: Any, depth: int = 0) -> Any:
    """Copy JSON-shaped input without coercing keys, scalars, or arbitrary objects."""
    if depth > MAX_DEPTH:
        raise ValueError(f"JSON nesting exceeds {MAX_DEPTH}")
    if type(value) is str:
        try:
            if len(value) > MAX_DOCUMENT_BYTES or len(value.encode("utf-8")) > MAX_DOCUMENT_BYTES:
                raise ValueError("JSON string exceeds the byte limit")
        except UnicodeError as error:
            raise ValueError("JSON string contains invalid Unicode") from error
        return value
    if value is None or type(value) is bool:
        return value
    if type(value) is int:
        if value.bit_length() > MAX_INTEGER_BITS:
            raise ValueError(f"JSON integer exceeds {MAX_INTEGER_BITS} bits")
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("JSON numbers must be finite")
        return value
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise ValueError("JSON object keys must be strings")
        return {plain(key, depth + 1): plain(item, depth + 1) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item, depth + 1) for item in value]
    raise ValueError(f"Unsupported JSON value type: {type(value).__name__}")


def encoded(value: Any) -> bytes:
    try:
        data = json.dumps(
            plain(value), sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (RecursionError, UnicodeError) as error:
        raise ValueError("JSON contains excessive nesting or invalid Unicode") from error
    if len(data) > MAX_DOCUMENT_BYTES:
        raise ValueError(f"JSON document exceeds {MAX_DOCUMENT_BYTES} bytes")
    return data


def frozen_object(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a JSON object")
    data = plain(value)
    encoded(data)

    def freeze(item: Any) -> Any:
        if isinstance(item, dict):
            return MappingProxyType({key: freeze(child) for key, child in item.items()})
        if isinstance(item, list):
            return tuple(freeze(child) for child in item)
        return item

    return freeze(data)


def loads(raw: bytes | str) -> Any:
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise ValueError(f"JSON document exceeds {MAX_DOCUMENT_BYTES} bytes")
    if isinstance(raw, str):
        try:
            raw = raw.encode("utf-8")
        except UnicodeError as error:
            raise ValueError("JSON text contains invalid Unicode") from error
        if len(raw) > MAX_DOCUMENT_BYTES:
            raise ValueError(f"JSON document exceeds {MAX_DOCUMENT_BYTES} bytes")

    def pairs(items):
        result = {}
        for key, item in items:
            if key in result:
                raise ValueError(f"Duplicate JSON key: {key}")
            result[key] = item
        return result

    def finite(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("JSON numbers must be finite")
        return result

    def reject(value):
        raise ValueError(f"Invalid JSON constant: {value}")

    try:
        return plain(
            json.loads(raw, object_pairs_hook=pairs, parse_float=finite, parse_constant=reject)
        )
    except (RecursionError, UnicodeError) as error:
        raise ValueError("JSON contains excessive nesting or invalid Unicode") from error


def load(path: str | Path) -> Any:
    with Path(path).open("rb") as stream:
        return loads(stream.read(MAX_DOCUMENT_BYTES + 1))
