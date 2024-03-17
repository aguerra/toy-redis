"""Server implementation."""

from asyncio import (
    run as asyncio_run,
    StreamReader,
    StreamWriter,
    start_server,
)
from collections.abc import MutableMapping
from functools import partial

from .command import CommandError, run
from .wire import dump, load, Error

Storage = MutableMapping[bytes, bytes]


async def _command_loop(storage: Storage,
                        reader: StreamReader,
                        writer: StreamWriter
                        ) -> None:
    while (command := await load(reader)):
        try:
            response = run(storage, command)
        except CommandError as e:
            response = Error(str(e))  # type: ignore
        await dump(response, writer)
    writer.close()
    await writer.wait_closed()


async def start_and_serve_forever() -> None:
    """Create the server and start serving requests."""
    callback = partial(_command_loop, {})
    server = await start_server(callback, '127.0.0.1', 8000)
    await server.serve_forever()

if __name__ == '__main__':
    asyncio_run(start_and_serve_forever())
