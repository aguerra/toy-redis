from asyncio import StreamReader
from unittest import mock

import pytest

from toy_redis.wire import dump, load_command, LoadError


@pytest.fixture
def stream_reader():
    return StreamReader()


@pytest.fixture
def buf():
    return bytearray()


@pytest.fixture
def stream_writer(buf):
    def write(data):
        buf.extend(data)

    stream_writer = mock.Mock()
    stream_writer.write = write
    stream_writer.drain = mock.AsyncMock()
    return stream_writer


@pytest.fixture
def command():
    return [b'command', b'arg']


@pytest.fixture
def serialized_command():
    return b'*2\r\n$7\r\ncommand\r\n$3\r\narg\r\n'


@pytest.fixture
def obj():
    return [None, b'abc', 'de', 42, Exception('Error')]


@pytest.fixture
def serialized_obj():
    return b'*5\r\n$-1\r\n$3\r\nabc\r\n+de\r\n:42\r\n-Error\r\n'


async def test_load_valid_command(command, serialized_command, stream_reader):
    stream_reader.feed_data(serialized_command)
    got = await load_command(stream_reader)
    assert command == got


async def test_load_eof_is_empty_command(stream_reader):
    stream_reader.feed_eof()
    got = await load_command(stream_reader)
    assert [] == got


async def test_load_invalid_command(stream_reader):
    data = '%-8a7'.encode()
    stream_reader.feed_data(data)
    stream_reader.feed_eof()
    with pytest.raises(LoadError) as excinfo:
        await load_command(stream_reader)
    assert 'Invalid data' in str(excinfo)


async def test_dump_supported_types(buf, obj, serialized_obj, stream_writer):
    await dump(obj, stream_writer)
    assert serialized_obj == buf
