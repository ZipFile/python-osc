"""OSC Servers that receive TCP packets and invoke handlers accordingly.

Use like this:

dispatcher = dispatcher.Dispatcher()
# This will print all parameters to stdout.
dispatcher.map("/bpm", print)
server = ForkingOSCTCPServer((ip, port), dispatcher)
server.serve_forever()

or run the server on its own thread:
server = ForkingOSCTCPServer((ip, port), dispatcher)
server_thread = threading.Thread(target=server.serve_forever)
server_thread.start()
...
server.shutdown()


Those servers are using the standard socketserver from the standard library:
http://docs.python.org/library/socketserver.html


Alternatively, the AsyncIOOSCTCPServer server can be integrated with an
asyncio event loop:

loop = asyncio.get_event_loop()
server = AsyncIOOSCTCPServer(server_address, dispatcher)
server.serve()
loop.run_forever()

"""

# mypy: disable-error-code="attr-defined"

import asyncio
import os
import socket
import socketserver
from typing import Tuple

from pythonosc import osc_message_builder
from pythonosc.dispatcher import Dispatcher
from pythonosc.parsing.framing import (
    BINARY_LENGTH_FRAMING,
    SLIP_FRAMING,
    Framing,
    FramingParser,
)

MODE_1_0 = "1.0"
MODE_1_1 = "1.1"
FRAMING_BY_MODE: dict[str, Framing] = {
    MODE_1_0: BINARY_LENGTH_FRAMING,
    MODE_1_1: SLIP_FRAMING,
}


def get_framing(mode: str) -> Framing:
    try:
        return FRAMING_BY_MODE[mode]
    except KeyError:
        raise ValueError(f"Unsupported OSC mode: {mode}") from None


class TCPHandler(socketserver.BaseRequestHandler):
    """Handles correct OSC messages.

    Whether this will be run on its own thread, the server's or a whole new
    process depends on the server you instantiated, look at their documentation.

    This method is called after a basic sanity check was done on the datagram,
    basically whether this datagram looks like an osc message or bundle,
    if not the server won't even bother to call it and so no new
    threads/processes will be spawned.
    """

    def handle(self) -> None:
        assert isinstance(self.server, OSCTCPServer)

        parser = FramingParser(self.server.framing)

        while chunk := self.request.recv(16384):
            for p in parser.feed(chunk):
                resp = self.server.dispatcher.call_handlers_for_packet(
                    p, self.client_address
                )
                for r in resp:
                    if not isinstance(r, tuple):
                        r = [r]
                    msg = osc_message_builder.build_msg(r[0], r[1:])
                    self.request.sendall(self.server.framing.encode(msg.dgram))


class OSCTCPServer(socketserver.TCPServer):
    """Superclass for different flavors of OSCTCPServer"""

    dispatcher: Dispatcher
    framing: Framing

    def __init__(
        self,
        server_address: Tuple[str | bytes | bytearray, int],
        dispatcher: Dispatcher,
        mode: str = MODE_1_1,
        family: socket.AddressFamily | None = None,
    ):
        self.request_queue_size = 300
        self.dispatcher = dispatcher
        self.framing = get_framing(mode)

        if family is not None:
            self.address_family = family
        elif isinstance(server_address[0], str):
            # Try to infer address family from server_address
            try:
                infos = socket.getaddrinfo(
                    server_address[0],
                    server_address[1],
                    type=socket.SOCK_STREAM,
                    family=socket.AF_UNSPEC,
                )
                if infos:
                    self.address_family = infos[0][0]
            except (socket.gaierror, IndexError):
                # Fallback to default if resolution fails
                pass

        super().__init__(server_address, TCPHandler)


class BlockingOSCTCPServer(OSCTCPServer):
    """Blocking version of the TCP server.

    Each message will be handled sequentially on the same thread.
    Use this is you don't care about latency in your message handling or don't
    have a multiprocess/multithread environment (really?).
    """


class ThreadingOSCTCPServer(socketserver.ThreadingMixIn, OSCTCPServer):
    """Threading version of the OSC TCP server.

    Each message will be handled in its own new thread.
    Use this when lightweight operations are done by each message handlers.
    """


if hasattr(os, "fork"):

    class ForkingOSCTCPServer(socketserver.ForkingMixIn, OSCTCPServer):
        """Forking version of the OSC TCP server.

        Each message will be handled in its own new process.
        Use this when heavyweight operations are done by each message handlers
        and forking a whole new process for each of them is worth it.
        """


class AsyncOSCTCPServer:
    """Asyncio version of the OSC TCP Server.
    Each TCP message is handled by _call_handlers_for_packet, the same method as in the
    OSCTCPServer family of blocking, threading, and forking servers
    """

    def __init__(
        self,
        server_address: str,
        port: int,
        dispatcher: Dispatcher,
        mode: str = MODE_1_1,
    ):
        """
        :param server_address: tuple of (IP address to bind to, port)
        :param dispatcher: a pythonosc.dispatcher.Dispatcher
        """
        self._port = port
        self._server_address = server_address
        self.dispatcher = dispatcher
        self.framing = get_framing(mode)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def start(self) -> None:
        """creates a socket endpoint and registers it with our event loop"""
        self._server = await asyncio.start_server(
            self.handle, self._server_address, self._port
        )

        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        self._server.close()
        await self._server.wait_closed()

    async def handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            await self._handle(reader, writer)
        finally:
            if writer.can_write_eof():
                writer.write_eof()
            writer.close()
            await writer.wait_closed()

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        parser = FramingParser(self.framing)
        client_address = ("", 0)
        sock = writer.transport.get_extra_info("socket")
        if sock is not None:
            client_address = sock.getpeername()

        while chunk := await reader.read(16384):
            for p in parser.feed(chunk):
                result = await self.dispatcher.async_call_handlers_for_packet(
                    p, client_address
                )
                for r in result:
                    if not isinstance(r, tuple):
                        r = [r]
                    msg = osc_message_builder.build_msg(r[0], r[1:])
                    writer.write(self.framing.encode(msg.dgram))
                    await writer.drain()
