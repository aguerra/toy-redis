"""Server implementation."""

import logging

from asyncio import (
    run,
    StreamReader,
    StreamWriter,
    start_server,
)
from collections.abc import MutableMapping
from functools import partial

from .command import cast_to_command_and_run, CommandError
from .wire import dump, load, LoadError, ProtocolError, Serializable

Storage = MutableMapping[bytes, bytes]

logger = logging.getLogger(__name__)


async def _command_loop(storage: Storage,
                        reader: StreamReader,
                        writer: StreamWriter,
                        address: str
                        ) -> None:
    while request :=  await load(reader):
        logger.debug(f'request={request} address={address}')
        try:
            response = cast_to_command_and_run(request, storage)
        except CommandError as e:
            logger.exception(f'address={address}')
            response = ProtocolError(str(e))  # type: ignore
        logger.debug(f'response={response!r} address={address}')
        await dump(response, writer)
    logger.debug(f'request=EOF address={address}')


async def _log_exception_and_dump(response: Serializable,
                                  writer: StreamWriter,
                                  address: str
                                  ) -> None:
    logger.exception(f'address={address}')
    await dump(response, writer)


async def _start_server_callback(storage: Storage,
                                 reader: StreamReader,
                                 writer: StreamWriter
                                 ) -> None:
    address = writer.get_extra_info('peername')
    try:
        await _command_loop(storage, reader, writer, address)
    except LoadError as e:
        response = ProtocolError(str(e))
        await _log_exception_and_dump(response, writer, address)
    except:
        response = ProtocolError('Internal server error')
        await _log_exception_and_dump(response, writer, address)
    writer.close()
    await writer.wait_closed()


async def start_and_serve_forever() -> None:
    """Create the server and start serving requests."""
    callback = partial(_start_server_callback, {})
    server = await start_server(callback, '127.0.0.1', 8000)
    await server.serve_forever()

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    )
    run(start_and_serve_forever())
