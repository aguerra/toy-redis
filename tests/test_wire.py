from asyncio import StreamReader
from unittest import mock

import pytest

from toy_redis.wire import dump, load, LoadError, ProtocolError


@pytest.fixture
def stream_reader():
    return StreamReader()


@pytest.fixture
def limited_stream_reader():
    return StreamReader(limit=1)


@pytest.fixture
def stream_writer_buffer():
    return bytearray()


@pytest.fixture
def stream_writer(stream_writer_buffer):
    def write(data):
        stream_writer_buffer.extend(data)

    stream_writer = mock.Mock()
    stream_writer.write = write
    stream_writer.drain = mock.AsyncMock()
    return stream_writer


@pytest.fixture
def obj():
    return None, ('z',), b'abc', 'de', 42, ProtocolError('Failed')


@pytest.fixture
def serialized_obj():
    return b'*6\r\n$-1\r\n*1\r\n+z\r\n$3\r\nabc\r\n+de\r\n:42\r\n-Failed\r\n'


async def test_load_supported_types(obj, serialized_obj, stream_reader):
    stream_reader.feed_data(serialized_obj)
    got = await load(stream_reader)
    assert got == obj


async def test_load_eof(stream_reader):
    stream_reader.feed_eof()
    got = await load(stream_reader)
    assert b'' == got


async def test_load_invalid_prefix(stream_reader):
    data = b'%-8a7'
    stream_reader.feed_data(data)
    with pytest.raises(LoadError) as excinfo:
        await load(stream_reader)
    assert 'Invalid prefix' in str(excinfo)


async def test_load_missing_separator(stream_reader):
    data = b'*6$-1'
    stream_reader.feed_data(data)
    stream_reader.feed_eof()
    with pytest.raises(LoadError) as excinfo:
        await load(stream_reader)
    assert 'Missing separator' in str(excinfo)


async def test_load_invalid_encoding(stream_reader):
    data = b'*6\xa0\r\n'
    stream_reader.feed_data(data)
    with pytest.raises(LoadError) as excinfo:
        await load(stream_reader)
    assert 'Invalid encoding' in str(excinfo)


async def test_load_invalid_number(stream_reader):
    data = b':b\r\n'
    stream_reader.feed_data(data)
    with pytest.raises(LoadError) as excinfo:
        await load(stream_reader)
    assert 'Invalid number' in str(excinfo)


async def test_load_request_too_big(limited_stream_reader):
    data = b':42\r\n'
    limited_stream_reader.feed_data(data)
    with pytest.raises(LoadError) as excinfo:
        await load(limited_stream_reader)
    assert 'Request is too big' in str(excinfo)


async def test_dump_supported_types(stream_writer_buffer,
                                    obj,
                                    serialized_obj,
                                    stream_writer):
    await dump(obj, stream_writer)
    assert serialized_obj == stream_writer_buffer
