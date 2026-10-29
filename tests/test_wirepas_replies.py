"""Wirepas SMP request matching after timeouts and device restarts."""

import asyncio
import subprocess
import sys
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from smpclient.requests.os_management import EchoWrite
from smpclient.transport.wirepas import SMPWirepasTransport


def test_import_does_not_load_optional_native_wirepas_dependencies():
    subprocess.run(
        [sys.executable, "-c", "import smpclient.transport.wirepas; "
         "import sys; assert 'wsctrl' not in sys.modules; "
         "assert 'dbusCExtension' not in sys.modules"],
        check=True,
    )


@pytest.mark.asyncio
async def test_connect_loads_and_initializes_controller(monkeypatch):
    factory = MagicMock()
    monkeypatch.setitem(sys.modules, "wsctrl", SimpleNamespace())
    monkeypatch.setitem(sys.modules, "wsctrl.sink_ctrl", SimpleNamespace(SinkController=factory))
    value = SMPWirepasTransport()
    await value.connect(21, 5)
    factory.assert_called_once_with(21, ep_src=70, ep_dst=84)
    factory.return_value.initialize_sink.assert_called_once_with()


def packet(payload, src=21, src_ep=84, dst_ep=70):
    return SimpleNamespace(payload=payload, src=src, src_ep=src_ep, dst_ep=dst_ep)


@pytest.fixture
def transport():
    queue = asyncio.Queue()
    value = SMPWirepasTransport()
    value._address = 21
    value._client = SimpleNamespace(send=MagicMock(), async_receive=queue.get)
    return value, queue


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["sequence", "group_id", "command_id", "op"])
async def test_ignore_unrelated_reply(transport, field):
    value, queue = transport
    request = EchoWrite(d="current")
    reply = request._Response(r="current", sequence=request.header.sequence)
    wrong_header = replace(reply.header, **{field: (getattr(reply.header, field) + 1) % 4})
    queue.put_nowait(packet(bytes(wrong_header) + reply.BYTES[8:]))
    queue.put_nowait(packet(reply.BYTES))
    assert await value.send_and_receive(request.BYTES) == reply.BYTES
    value._client.send.assert_called_once_with(request.BYTES)


@pytest.mark.asyncio
async def test_timeout_does_not_poison_next_request(transport):
    value, queue = transport
    expired = EchoWrite(d="expired")
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(value.send_and_receive(expired.BYTES), 0.01)
    current = EchoWrite(d="current")
    queue.put_nowait(packet(expired._Response(r="expired", sequence=expired.header.sequence).BYTES))
    reply = current._Response(r="current", sequence=current.header.sequence).BYTES
    queue.put_nowait(packet(reply))
    assert await asyncio.wait_for(value.send_and_receive(current.BYTES), 0.1) == reply


@pytest.mark.asyncio
async def test_source_and_endpoint_filtering_during_fragment_reassembly(transport):
    value, queue = transport
    request = EchoWrite(d="fragmented")
    reply = request._Response(r="fragmented", sequence=request.header.sequence).BYTES
    queue.put_nowait(packet(reply, src=22))
    queue.put_nowait(packet(reply, src_ep=66))
    queue.put_nowait(packet(reply, dst_ep=77))
    queue.put_nowait(packet(reply[:10]))
    queue.put_nowait(packet(b"unrelated fragment", src=22))
    queue.put_nowait(packet(reply[10:]))
    assert await value.send_and_receive(request.BYTES) == reply


@pytest.mark.asyncio
async def test_overlapping_requests_are_serialized(transport):
    value, queue = transport
    first, second = EchoWrite(d="first"), EchoWrite(d="second")
    replies = [req._Response(r=req.d, sequence=req.header.sequence).BYTES for req in (first, second)]
    first_task = asyncio.create_task(value.send_and_receive(first.BYTES))
    await asyncio.sleep(0)
    second_task = asyncio.create_task(value.send_and_receive(second.BYTES))
    await asyncio.sleep(0)
    assert value._client.send.call_count == 1
    queue.put_nowait(packet(replies[0]))
    assert await first_task == replies[0]
    await asyncio.sleep(0)
    assert value._client.send.call_count == 2
    queue.put_nowait(packet(replies[1]))
    assert await second_task == replies[1]
