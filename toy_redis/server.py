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

logger = logging.getLogger(__name__)

async def _load_request(reader: StreamReader, address: str) -> Serializable:
    try:
        request = await load(reader)
    except LoadError as e:
        request = ProtocolError(str(e))
    except Exception as e:
        message = 'Internal server error'
        request = ProtocolError(message)
        logger.exception(f'address={address} message={message}')
    return request


async def _command_loop(storage: MutableMapping[bytes, bytes],
                        reader: StreamReader,
                        writer: StreamWriter
                        ) -> None:
    address = writer.get_extra_info('peername')
    while (request := await _load_request(reader, address)):
        logger.debug(f'request={request} address={address}')
        try:
            response = cast_to_command_and_run(request, storage)
        except CommandError as e:
            logger.exception(f'address={address}')
            response = ProtocolError(str(e))  # type: ignore
        logger.debug(f'response={response!r} address={address}')
        await dump(response, writer)
    logger.debug(f'request=EOF address={address}')
    writer.close()
    await writer.wait_closed()


async def start_and_serve_forever() -> None:
    """Create the server and start serving requests."""
    callback = partial(_command_loop, {})
    server = await start_server(callback, '127.0.0.1', 8000)
    await server.serve_forever()

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    )
    run(start_and_serve_forever())
