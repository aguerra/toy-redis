from asyncio import StreamReader
from unittest import mock

import pytest

from toy_redis.wire import dump, load, LoadError


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
def obj():
    return [None, b'abc', 'de', 42, Exception('Error')]


@pytest.fixture
def serialized_obj():
    return b'*5\r\n$-1\r\n$3\r\nabc\r\n+de\r\n:42\r\n-Error\r\n'


async def test_load_supported_types(obj, serialized_obj, stream_reader):
    stream_reader.feed_data(serialized_obj)
    *objs, error = await load(stream_reader)
    assert objs == [None, b'abc', 'de', 42]
    assert isinstance(error, Exception)
    assert str(error) == 'Error'


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


async def test_dump_supported_types(buf, obj, serialized_obj, stream_writer):
    await dump(obj, stream_writer)
    assert serialized_obj == buf
