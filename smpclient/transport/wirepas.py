"""A Wirepas DualMCU SMPTransport.

"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Final

from smp import header as smphdr
from typing_extensions import override

from smpclient.exceptions import SMPClientException
from smpclient.transport import SMPTransport, SMPTransportDisconnected

if TYPE_CHECKING:
    from wsctrl.sink_ctrl import SinkController, WirepasResponse

logger = logging.getLogger(__name__)


class SMPWirepasTransportException(SMPClientException):
    """Base class for SMP Wirepas transport exceptions."""


class SMPWirepasTransportDeviceNotFound(SMPWirepasTransportException):
    """Raised when a Wirepas Dualmcu device is not found."""


class SMPWirepasTransportNotSMPServer(SMPWirepasTransportException):
    """Raised when the SMP server is not found on remote device."""


logger = logging.getLogger(__name__)


class SMPWirepasTransport(SMPTransport):
    """ Source and Destination enpoints may vary; These should be used as default for SMP. """
    WIREPAS_CMESH_FOTA_ENDPOINT_SRC = 70
    WIREPAS_CMESH_FOTA_ENDPOINT_DST = 84

    def __init__(self, mtu: int = 512) -> None:
        self._buffer = bytearray()
        self._nbors = []
        self._client: SinkController | None = None
        self._address: int | None = None
        self._request_lock = asyncio.Lock()
        # MTU = 512B is the most optimal and well-tested
        assert mtu == 512
        self._mtu = mtu
        """Initially set max MTU supported by Wirepas transport; may be mutated by the `connect()` method."""



    @override
    async def connect(self, address: int, timeout_s: float) -> None:
        # Loading wsctrl also loads native D-Bus support. Keep this optional
        # until a real Wirepas connection is requested.
        from wsctrl.sink_ctrl import SinkController

        # Address is uint32_t
        self._address = address & 0xFFFFFFFF
        self._client = SinkController(
            self._address,
            ep_src=self.WIREPAS_CMESH_FOTA_ENDPOINT_SRC,
            ep_dst=self.WIREPAS_CMESH_FOTA_ENDPOINT_DST,
        )
        self._client.initialize_sink()

    @override
    def send(self, data: bytes) -> None:
        logger.debug(f"Sending {len(data)} bytes")
        for offset in range(0, len(data), self.mtu):
            self._client.send(
                data[offset : offset + self.mtu]
            )
        logger.debug(f"Sent {len(data)} bytes")

    @override
    async def receive(self) -> bytes:
        response = await self._receive_from_peer()
        first_packet: Final = response.payload
        logger.debug(f"Received {len(first_packet)} B")

        header: Final = smphdr.Header.loads(first_packet[: smphdr.Header.SIZE])
        logger.debug(f"Received {header=}")

        message_length: Final = header.length + smphdr.Header.SIZE
        message: Final = bytearray(first_packet)

        if len(message) != message_length:
            logger.debug(f"Waiting for the rest of the {message_length} B response")
            while len(message) < message_length:
                response = await self._receive_from_peer()
                logger.debug(f"Received {len(response.payload)} B")
                message.extend(response.payload)
            if len(message) > message_length:
                error: Final = (
                    f"Received more data than expected: {len(message)} B > {message_length} B"
                )
                logger.error(error)
                raise SMPClientException(error)

        logger.debug(f"Finished receiving message of length {message_length} B")
        return message

    async def _receive_from_peer(self) -> WirepasResponse:
        if self._client is None:
            raise SMPTransportDisconnected("Wirepas transport is not connected")
        while True:
            response = await self._client.async_receive()
            if (
                response.src == self._address
                and response.src_ep == self.WIREPAS_CMESH_FOTA_ENDPOINT_DST
                and response.dst_ep == self.WIREPAS_CMESH_FOTA_ENDPOINT_SRC
            ):
                return response

    async def send_and_receive(self, data: bytes) -> bytes:
        request = smphdr.Header.loads(data[: smphdr.Header.SIZE])
        # Keep one request in flight so a reply cannot be consumed by a sibling request.
        # The caller's request timeout bounds both this lock and the receive loop.
        async with self._request_lock:
            self.send(data)
            while True:
                response = await self.receive()
                header = smphdr.Header.loads(response[: smphdr.Header.SIZE])
                if (
                    header.sequence == request.sequence
                    and header.group_id == request.group_id
                    and header.command_id == request.command_id
                    and header.op == request.op + 1
                ):
                    return response
                # A timed-out request can still finish after the device rejoins the mesh.
                logger.debug("Discarding delayed or unrelated Wirepas SMP reply: %s", header)

    @override
    @property
    def mtu(self) -> int:
        return self._mtu
