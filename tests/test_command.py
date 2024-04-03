import pytest

from toy_redis.command import (
    cast_to_command_and_run,
    CommandNotImplementedError,
    InvalidCommandError,
)

@pytest.fixture
def storage():
    return {}


def test_get(storage):
    key = b'key'
    expected = b'value'
    storage[key] = expected
    got = cast_to_command_and_run([b'GET', key], storage)
    assert expected == got


def test_set(storage):
    key = b'key'
    expected = b'value'
    got = cast_to_command_and_run([b'SET', key, expected], storage)
    assert 'OK' == got
    assert storage[key] == expected


def test_mget(storage):
    key1 = b'key1'
    key2 = b'key2'
    storage[key1] = b'value1'
    storage[key2] = b'value2'
    got = cast_to_command_and_run([b'MGET', key1, key2], storage)
    assert (b'value1', b'value2') == got


def test_mset(storage):
    key1 = b'key1'
    key2 = b'key2'
    data = [b'MSET', key1, b'value1', key2, b'value2']
    got = cast_to_command_and_run(data, storage)
    assert 'OK' == got
    assert storage[key1] == b'value1'
    assert storage[key2] == b'value2'


def test_del(storage):
    key = b'key'
    storage[key] = 42
    got = cast_to_command_and_run([b'DEL', key], storage)
    assert 1 == got
    assert storage.get(key) is None


def test_flushdb(storage):
    key = 'key'
    storage[key] = 42
    got = cast_to_command_and_run([b'FLUSHDB'], storage)
    assert 'OK' == got
    assert storage == {}


def test_command_not_implemented(storage):
    with pytest.raises(CommandNotImplementedError) as excinfo:
        cast_to_command_and_run([b'INVALID'], storage)
    assert 'command invalid' in str(excinfo)


def test_invalid_command_not_iterable(storage):
    with pytest.raises(InvalidCommandError) as excinfo:
        cast_to_command_and_run(42, storage)
    assert 'data is not iterable' in str(excinfo)


def test_invalid_command_name_or_args(storage):
    with pytest.raises(InvalidCommandError) as excinfo:
        cast_to_command_and_run(['key', 'value'], storage)
    assert 'command_name or arguments are not bytes' in str(excinfo)
