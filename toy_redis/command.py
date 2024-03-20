"""Server commands implementation."""

from itertools import pairwise
from typing import Any

Storage = dict[bytes, bytes]
Result = str | int | bytes | None | list[bytes | None]


def _get(storage: Storage, args: list[bytes]) -> bytes | None:
    key, *_ = args
    return storage.get(key)


def _mget(storage: Storage, args: list[bytes]) -> list[bytes | None]:
    result = [storage.get(key) for key in args]
    return result


def _set(storage: Storage, args: list[bytes]) -> str:
    key, value, *_ = args
    storage[key] = value
    return 'OK'


def _mset(storage: Storage, args: list[bytes]) -> str:
    for key, value in pairwise(args):
        storage[key] = value
    return 'OK'


def _del(storage: Storage, args: list[bytes]) -> int:
    n = 0
    for key in args:
        if storage.pop(key, None):
            n += 1
    return n


def _flushdb(storage: Storage, args: list[bytes]) -> str:
    storage.clear()
    return 'OK'


class CommandError(Exception):
    pass


class InvalidCommandError(CommandError):
    pass


class CommandNotImplementedError(CommandError):
    pass


def _command_name_and_args(command: Any) -> tuple[bytes, list[bytes]]:
    try:
        command_name, *args = command
    except TypeError as e:
        raise InvalidCommandError('command is not iterable ') from e
    if not all(isinstance(elem, bytes) for elem in command):
        raise InvalidCommandError('command name or arguments are not bytes')
    return command_name, args


def run(storage: Storage, command: Any) -> Result:
    """Run a server command."""
    command_name, args = _command_name_and_args(command)
    cmd_name_normalized = command_name.decode().lower()
    func = globals().get('_' + cmd_name_normalized)
    if func is None:
        raise CommandNotImplementedError(f'command {cmd_name_normalized}')
    return func(storage, args)
