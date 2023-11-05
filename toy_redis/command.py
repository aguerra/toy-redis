"""Handle commands."""

from collections.abc import Callable, MutableMapping, Sequence
from itertools import pairwise

Storage = MutableMapping[bytes, bytes]
Result = str | int | bytes | None | Sequence[bytes | None]
Command = Callable[[Storage, Sequence[bytes]], Result]


def _get(storage: Storage, args: Sequence[bytes]) -> bytes | None:
    key, *_ = args
    return storage.get(key)


def _mget(storage: Storage, args: Sequence[bytes]) -> Sequence[bytes | None]:
    result = [storage.get(key) for key in args]
    return result


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


def run(storage: Storage, run_args: Sequence[bytes]) -> Result:
    """Run command."""
    command, *args = run_args
    command_name = command.decode().lower()
    func: Command
    match command_name:
        case 'get':
            func = _get
        case 'set':
            func = _set
        case 'mget':
            func = _mget
        case 'mset':
            func = _mset
        case 'del':
            func = _del
        case 'flushdb':
            func = _flushdb
        case _:
            raise CommandError('Command not implemented')
    return func(storage, args)
