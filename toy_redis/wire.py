"""Serialization and deserialization for the wire protocol."""

from asyncio import StreamReader, StreamWriter
from collections.abc import Sequence
from functools import singledispatch
from typing import NamedTuple


class Error(NamedTuple):
    message: str


SerializableSequence = Sequence['Serializable']
Serializable = None | int | str | bytes | Error | SerializableSequence


async def _decode_until_crlf(reader: StreamReader) -> str:
    data = await reader.readuntil(b'\r\n')
    trimmed = data[:-2]
    return trimmed.decode()


async def _decode_integer(reader: StreamReader) -> int:
    string = await _decode_until_crlf(reader)
    return int(string)


async def _load_bulk_string(reader: StreamReader) -> bytes | None:
    if (length := await _decode_integer(reader)) == -1:
        return None
    data = await reader.read(length + 2)
    return data[:-2]


async def _load_array(reader: StreamReader) -> SerializableSequence:
    length = await _decode_integer(reader)
    obj = [await _load(reader) for _ in range(length)]
    return tuple(obj)


async def _load_string(reader: StreamReader) -> str:
    return await _decode_until_crlf(reader)


async def _load_error(reader: StreamReader) -> Error:
    string = await _decode_until_crlf(reader)
    return Error(string)


async def _load(reader: StreamReader) -> Serializable:
    if not (prefix := await reader.read(1)):
        return b''
    match prefix:
        case b'*':
            return await _load_array(reader)
        case b'$':
            return await _load_bulk_string(reader)
        case b'+':
            return await _load_string(reader)
        case b':':
            return await _decode_integer(reader)
        case b'-':
            return await _load_error(reader)
        case _:
            return Error(f'Unsupported prefix {prefix!r}')


async def load(reader: StreamReader) -> Serializable:
    """Deserialize from reader to a Python object."""
    obj = await _load(reader)
    return obj


@singledispatch
def _to_bytes(_: Serializable) -> bytes:
    return b''


@_to_bytes.register
def _(obj: str | int) -> bytes:
    prefix = '+' if isinstance(obj, str) else ':'
    data = f'{prefix}{obj}\r\n'.encode()
    return data


@_to_bytes.register
def _(obj: None) -> bytes:
    data = '$-1\r\n'.encode()
    return data


@_to_bytes.register
def _(obj: bytes) -> bytes:
    length = len(obj)
    header = f'${length}\r\n'.encode()
    data = b''.join([header, obj, b'\r\n'])
    return data


@_to_bytes.register
def _(obj: Error) -> bytes:
    message = obj.message
    data = f'-{message}\r\n'.encode()
    return data


@_to_bytes.register
def _(obj: Sequence) -> bytes:
    length = len(obj)
    header = f'*{length}\r\n'.encode()
    items_data = b''.join(_to_bytes(item) for item in obj)
    data = b''.join([header, items_data])
    return data


async def dump(obj: Serializable, writer: StreamWriter) -> None:
    """Serialize obj to writer."""
    data = _to_bytes(obj)
    writer.write(data)
    await writer.drain()
