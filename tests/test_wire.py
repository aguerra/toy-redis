from asyncio import StreamReader
from unittest import mock

import pytest

from toy_redis.wire import dump, load, Error, LoadError


@pytest.fixture
def stream_reader():
    return StreamReader()


@pytest.fixture
def buffer():
    return bytearray()


@pytest.fixture
def stream_writer(buffer):
    def write(data):
        buffer.extend(data)

    stream_writer = mock.Mock()
    stream_writer.write = write
    stream_writer.drain = mock.AsyncMock()
    return stream_writer


@pytest.fixture
def obj():
    return [None, ['z'], b'abc', 'de', 42, Error('Error')]


@pytest.fixture
def serialized_obj():
    return b'*6\r\n$-1\r\n*1\r\n+z\r\n$3\r\nabc\r\n+de\r\n:42\r\n-Error\r\n'


async def test_load_supported_types(obj, serialized_obj, stream_reader):
    stream_reader.feed_data(serialized_obj)
    got = await load(stream_reader)
    assert got == obj


async def test_load_eof(stream_reader):
    stream_reader.feed_eof()
    got = await load(stream_reader)
    assert b'' == got


async def test_load_invalid_data(stream_reader):
    data = '%-8a7'.encode()
    stream_reader.feed_data(data)
    stream_reader.feed_eof()
    with pytest.raises(LoadError) as excinfo:
        await load(stream_reader)
    assert 'Invalid data' in str(excinfo)


async def test_dump_supported_types(buffer, obj, serialized_obj, stream_writer):
    await dump(obj, stream_writer)
    assert serialized_obj == buffer
