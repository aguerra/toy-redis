"""Server commands implementation."""

from collections.abc import MutableMapping, Sequence
from itertools import pairwise
from typing import Any

Storage = MutableMapping[bytes, bytes]
Response = str | int | bytes | None | Sequence[bytes | None]


def _get(storage: Storage, args: Sequence[bytes]) -> bytes | None:
    key, *_ = args
    return storage.get(key)


def _mget(storage: Storage, args: Sequence[bytes]) -> Sequence[bytes | None]:
    response = tuple(storage.get(key) for key in args)
    return response


def _set(storage: Storage, args: Sequence[bytes]) -> str:
    key, value, *_ = args
    storage[key] = value
    return 'OK'


def _mset(storage: Storage, args: Sequence[bytes]) -> str:
    for key, value in pairwise(args):
        storage[key] = value
    return 'OK'


def _del(storage: Storage, args: Sequence[bytes]) -> int:
    n = 0
    for key in args:
        if storage.pop(key, None):
            n += 1
    return n


def _flushdb(storage: Storage, args: Sequence[bytes]) -> str:
    storage.clear()
    return 'OK'


class CommandError(Exception):
    pass


class InvalidCommandError(CommandError):
    pass


class CommandNotImplementedError(CommandError):
    pass


def _command_name_and_args(data: Any) -> tuple[bytes, Sequence[bytes]]:
    try:
        command_name, *args = data
    except TypeError as e:
        raise InvalidCommandError('command is not iterable ') from e
    if not isinstance(command_name, bytes):
        raise InvalidCommandError('command_name is not bytes')
    if not all(isinstance(arg, bytes) for arg in args):
        raise InvalidCommandError('arguments are not bytes')
    return command_name, args


def cast_to_command_and_run(data: Any, storage: Storage) -> Response:
    """Cast data to server command and run it."""
    command_name, args = _command_name_and_args(data)
    cmd_name_normalized = command_name.decode().lower()
    func = globals().get('_' + cmd_name_normalized)
    if func is None:
        raise CommandNotImplementedError(f'command {cmd_name_normalized}')
    return func(storage, args)
