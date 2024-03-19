import pytest

from toy_redis.command import (
    CommandNotImplementedError,
    InvalidCommandError,
    run,
)

@pytest.fixture
def storage():
    return {}


def test_run_get(storage):
    key = b'key'
    expected = b'value'
    storage[key] = expected
    got = run(storage, [b'GET', key])
    assert expected == got


def test_run_set(storage):
    key = b'key'
    expected = b'value'
    got = run(storage, [b'SET', key, expected])
    assert 'OK' == got
    assert storage[key] == expected


def test_run_mget(storage):
    key1 = b'key1'
    key2 = b'key2'
    storage[key1] = b'value1'
    storage[key2] = b'value2'
    got = run(storage, [b'MGET', key1, key2])
    assert [b'value1', b'value2'] == got


def test_run_mset(storage):
    key1 = b'key1'
    key2 = b'key2'
    got = run(storage, [b'MSET', key1, b'value1', key2, b'value2'])
    assert 'OK' == got
    assert storage[key1] == b'value1'
    assert storage[key2] == b'value2'


def test_run_del(storage):
    key = b'key'
    storage[key] = 42
    got = run(storage, [b'DEL', key])
    assert 1 == got
    assert storage.get(key) is None


def test_run_flushdb(storage):
    key = 'key'
    storage[key] = 42
    got = run(storage, [b'FLUSHDB'])
    assert 'OK' == got
    assert storage == {}


def test_run_command_not_implemented(storage):
    with pytest.raises(CommandNotImplementedError) as excinfo:
        run(storage, [b'INVALID'])
    assert 'command invalid' in str(excinfo)


def test_run_invalid_command_not_iterable(storage):
    with pytest.raises(InvalidCommandError) as excinfo:
        run(storage, 42)
    assert 'command is not iterable' in str(excinfo)


def test_run_invalid_command_name_and_args(storage):
    with pytest.raises(InvalidCommandError) as excinfo:
        run(storage, ['key', 'value'])
    assert 'command name and arguments are not bytes' in str(excinfo)
