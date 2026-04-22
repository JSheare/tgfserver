"""A module containing classes that implement a simple string over TCP protocol."""
import asyncio
import socket
from typing import AsyncGenerator, Generator


class StringTCP:
    """A class that implements a simple string over TCP protocol."""
    _header_size_bytes = 4
    _max_recv = 1024
    _max_message_size = (2 ** (_header_size_bytes * 8)) - 1

    @classmethod
    def _encode_message(cls, message: str) -> bytes:
        """Encodes the given message, plus a length header, and returns it as bytes."""
        encoded_message = message.encode('utf-8')
        if len(encoded_message) > cls._max_message_size:
            raise ValueError('message too big to send.')

        header = len(encoded_message).to_bytes(cls._header_size_bytes, 'big', signed=False)
        return header + encoded_message

    @classmethod
    def send_message(cls, sock: socket.socket, message: str) -> None:
        """Sends the given message over the given socket.

        Parameters
        ----------
        sock : socket.socket
            The socket to send the message over.
        message : str
            The message to send.

        Raises
        ------
        ConnectionError
            If the socket is closed or broken.
        ValueError
            If the message can't be encoded or is too large to send.

        """

        try:
            sock.sendall(cls._encode_message(message))
        except (BrokenPipeError, ConnectionError):
            raise ConnectionError('socket is closed or broken.')

    @classmethod
    def get_messages(cls, sock: socket.socket, timeout: float | None = None) -> Generator[str, None, None]:
        """Returns a generator that can be iterated over for messages until the passed socket is closed.

        Parameters
        ----------
        sock : socket.socket
            The socket to get messages from.
        timeout : float | None, optional
            The amount of time to wait for data to be received over the socket. If no data arrives before the timeout,
            a TimeoutError is raised.

        Yields
        ------
        str
            Sent message strings.

        Raises
        ------
        TimeoutError
            If no data is received over the socket before the given timeout.
        ValueError
            If a message can't be decoded.

        """

        buffer = bytearray()
        message_length = None
        leftover = False
        closed = False
        while True:
            if closed:
                break

            if not leftover:
                try:
                    # Configuring the timeout if applicable
                    original_timeout = sock.gettimeout()
                    if timeout is not None:
                        sock.settimeout(timeout)


                    data = sock.recv(cls._max_recv)

                    # Restoring the timeout setting if applicable
                    if timeout is not None:
                        sock.settimeout(original_timeout)

                    if not data:
                        # Attempting to decode whatever is left in the buffer and then stopping
                        closed = True
                    else:
                        # Adding the new data to the buffer
                        buffer += bytearray(data)

                except (BrokenPipeError, ConnectionError):
                    closed = True

            else:
                leftover = False

            # Attempting to decode the message length header
            if message_length is None and len(buffer) >= cls._header_size_bytes:
                message_length = int.from_bytes(buffer[:cls._header_size_bytes], 'big', signed=False)
                buffer = buffer[cls._header_size_bytes:]
            else:
                continue

            # Attempting to decode the message content
            if message_length is not None and len(buffer) >= message_length:
                yield buffer[:message_length].decode('utf-8')
                buffer = buffer[message_length:]
                message_length = None
            else:
                continue

            # Checking to see if there's another full message in the buffer before waiting for more data
            if len(buffer) > 0:
                leftover = True


class AsyncStringTCP(StringTCP):
    """A class for asynchronous use that implements a simple string over TCP protocol."""
    @classmethod
    async def send_message(cls, writer: asyncio.StreamWriter, message: str) -> None:
        """Sends the given message over the given asyncio StreamWriter.

        Parameters
        ----------
        writer : asyncio.StreamWriter
            The asyncio StreamWriter to send the message over.
        message : str
            The message to send.

        Raises
        ------
        ConnectionError
            If the StreamWriter is closed, closing, or broken.
        ValueError
            If the message can't be encoded or is too large to send.

        """

        if not writer.is_closing():
            try:
                writer.write(cls._encode_message(message))
                await writer.drain()
            except (BrokenPipeError, ConnectionError):
                raise ConnectionError('socket is closed or broken.')

        else:
            raise ConnectionError('socket is closed or closing.')

    @classmethod
    async def get_messages(cls, reader: asyncio.StreamReader,
                           timeout: float | None = None) -> AsyncGenerator[str, None]:
        """Returns an async generator that can be iterated over for messages until the passed asyncio StreamReader is
        closed.

        Parameters
        ----------
        reader : asyncio.StreamReader
            The asyncio StreamReader to get messages from.
        timeout : float | None, optional
            The amount of time to wait for data to be received over the socket. If no data arrives before the timeout,
            a TimeoutError is raised.

        Yields
        ------
        str
            Sent message strings.

        Raises
        ------
        TimeoutError
            If no data is received over the socket before the given timeout.
        ValueError
            If a message can't be decoded.

        """

        buffer = bytearray()
        message_length = None
        leftover = False
        closed = False
        while True:
            if closed:
                break

            if not leftover:
                try:
                    # Waiting with a timeout if applicable
                    if timeout is not None:
                        try:
                            async with asyncio.timeout(timeout):
                                data = await reader.read(n=cls._max_recv)

                        except TimeoutError:
                            raise TimeoutError('socket connection timed out.')

                    else:
                        data = await reader.read(n=cls._max_recv)

                    if not data:
                        # Attempting to decode whatever is left in the buffer and then stopping
                        closed = True
                    else:
                        buffer += bytearray(data)

                except (BrokenPipeError, ConnectionError):
                    closed = True

            else:
                leftover = False

            # Attempting to decode the message length header
            if message_length is None and len(buffer) >= cls._header_size_bytes:
                message_length = int.from_bytes(buffer[:cls._header_size_bytes], 'big', signed=False)
                buffer = buffer[cls._header_size_bytes:]
            else:
                continue

            # Attempting to decode the message content
            if message_length is not None and len(buffer) >= message_length:
                yield buffer[:message_length].decode('utf-8')
                buffer = buffer[message_length:]
                message_length = None
            else:
                continue

            # Checking to see if there's another full message in the buffer before waiting for more data
            if len(buffer) > 0:
                leftover = True
