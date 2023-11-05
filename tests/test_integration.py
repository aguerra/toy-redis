import asyncio

import pytest
from redis.asyncio import Redis

from toy_redis.server import start_and_serve_forever


@pytest.fixture(scope='module')
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True, scope='module')
async def server(event_loop):
    task = asyncio.create_task(start_and_serve_forever())
    yield
    task.cancel()


@pytest.fixture
async def redis():
    r = Redis(host='localhost', port=8000)
    yield r
    await r.aclose()


async def test_run_set(redis):
    got = await redis.set('foo', 'bar')
    assert got is True


async def test_run_get(redis):
    got = await redis.get('foo')
    assert b'bar' == got


async def test_run_mset(redis):
    got = await redis.mset({'key1': 'value1', 'key2': 'value2'})
    assert got is True


async def test_run_mget(redis):
    got = await redis.mget('key1', 'key2', 'foo')
    assert [b'value1', b'value2', b'bar'] == got


async def test_run_delete(redis):
    key = 'key1'
    got = await redis.delete(key)
    value = await redis.get(key)
    assert 1 == got
    assert value is None


async def test_run_flushdb(redis):
    got = await redis.flushdb()
    values = await redis.mget('key2', 'foo')
    assert got is True
    assert [None, None] == values
