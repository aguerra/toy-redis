"""Server implementation."""

from asyncio import (
    run,
    StreamReader,
    StreamWriter,
    start_server,
)
from functools import partial

from .command import cast_to_command_and_run, CommandError
from .wire import dump, load, Error


async def _command_loop(storage: dict[bytes, bytes],
                        reader: StreamReader,
                        writer: StreamWriter
                        ) -> None:
    while (data := await load(reader)):
        try:
            response = cast_to_command_and_run(data, storage)
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
    run(start_and_serve_forever())
