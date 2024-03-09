"""Serialization and deserialization for the wire protocol."""

from asyncio import StreamReader, StreamWriter
from collections.abc import Sequence
from functools import singledispatch

SerializableSequence = Sequence['Serializable']
Serializable = None | int | str | bytes | Exception | SerializableSequence


async def _decode_until_crlf(reader: StreamReader) -> str:
    data = await reader.readuntil(b'\r\n')
    trimmed = data[:-2]
    return trimmed.decode()


def _validate_prefix(expected: bytes, prefix: bytes) -> None:
    if prefix != expected:
        raise ValueError('Wrong prefix {prefix!r}')


async def _load_bulk_string(reader: StreamReader) -> bytes:
    prefix = await reader.read(1)
    _validate_prefix(b'$', prefix)
    string = await _decode_until_crlf(reader)
    length = int(string) + 2
    data = await reader.read(length)
    return data[:-2]


async def _load_array_bulk_string(prefix: bytes,
                                  reader: StreamReader
                                  ) -> Sequence[bytes]:
    _validate_prefix(b'*', prefix)
    string = await _decode_until_crlf(reader)
    length = int(string)
    obj = [await _load_bulk_string(reader) for _ in range(length)]
    return obj


class LoadError(Exception):
    pass


async def load_command(reader: StreamReader) -> Sequence[bytes]:
    """Deserialize reader to a Python object representing a command."""
    prefix = await reader.read(1)
    if prefix == b'':
        return []
    try:
        obj = await _load_array_bulk_string(prefix, reader)
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
def _(obj: Exception) -> bytes:
    message = str(obj)
    data = f'-{message}\r\n'.encode()
    return data


@_to_bytes.register
def _(obj: Sequence) -> bytes:
    length = len(obj)
    header = f'*{length}\r\n'.encode()
    obj_data = b''.join(_to_bytes(item) for item in obj)
    data = b''.join([header, obj_data])
    return data


async def dump(obj: Serializable, writer: StreamWriter) -> None:
    """Serialize obj to writer."""
    data = _to_bytes(obj)
    writer.write(data)
    await writer.drain()
