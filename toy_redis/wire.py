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


async def _load_bulk_string(reader: StreamReader, length: int) -> bytes | None:
    if length == -1:
        return None
    data = await reader.read(length + 2)
    return data[:-2]


async def _load_array(reader: StreamReader,
                      length: int) -> SerializableSequence:
    obj = [await _load(reader) for _ in range(length)]
    return obj


async def _load(reader: StreamReader) -> Serializable:
    if not (prefix := await reader.read(1)):
        return b''
    decoded = await _decode_until_crlf(reader)
    match prefix:
        case b'*':
            return await _load_array(reader, int(decoded))
        case b'$':
            return await _load_bulk_string(reader, int(decoded))
        case b'+':
            return decoded
        case b':':
            return int(decoded)
        case b'-':
            return Error(decoded)
        case _:
            raise ValueError('Invalid prefix {prefix!r}')


class LoadError(Exception):
    pass


async def load(reader: StreamReader) -> Serializable:
    """Deserialize from reader to a Python object."""
    try:
        obj = await _load(reader)
    except Exception as e:
        raise LoadError('Invalid data') from e
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
