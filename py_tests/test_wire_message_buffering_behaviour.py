# Copyright 2026 The Action Engine Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import uuid

import actionengine
import pytest

# TODO: Use a local signalling server for tests
SIGNALLING_URL = "wss://actionengine.dev:19001"


def make_node_fragment(idx: int) -> actionengine.NodeFragment:
    metadata = actionengine.ChunkMetadata(mimetype="text/plain")
    chunk = actionengine.Chunk(metadata=metadata, data=f"{idx}".encode("utf-8"))
    node_fragment = actionengine.NodeFragment(
        continued=False,
        id=f"node_{idx}",
        seq=0,
        chunk=chunk,
    )
    return node_fragment


def make_webrtc_server_with_handler(
        handler,
        server_identity: str,
        preferred_port: int,
        run: bool = False,
) -> tuple[
    actionengine.ActionRegistry,
    actionengine.Service,
    actionengine.webrtc.WebRtcServer,
]:
    action_registry = actionengine.ActionRegistry()
    service = actionengine.Service(action_registry, handler)

    rtc_config = actionengine.webrtc.RtcConfig()
    rtc_config.preferred_port_range = (preferred_port, preferred_port)
    webrtc_server = actionengine.webrtc.WebRtcServer.create(
        service,
        "127.0.0.1",
        server_identity,
        SIGNALLING_URL,
        rtc_config,
    )

    if run:
        webrtc_server.run()

    return action_registry, service, webrtc_server


async def handle_connection_for_test_buffering_works(
        stream: actionengine._C.webrtc.WebRtcWireStream,
        _: actionengine.Session,
        recv_timeout: float = -1.0,
):
    received = await asyncio.to_thread(stream.receive, recv_timeout)
    assert received is None  # half-closed without messages

    with actionengine.buffer_wire_messages(stream):
        for idx in range(3):
            wire_message = actionengine.WireMessage()
            wire_message.node_fragments.append(make_node_fragment(idx))
            await asyncio.to_thread(stream.send, wire_message)

    for idx in range(3, 6):
        wire_message = actionengine.WireMessage()
        wire_message.node_fragments.append(make_node_fragment(idx))
        await asyncio.to_thread(stream.send, wire_message)

    await asyncio.to_thread(stream.half_close)


@pytest.mark.asyncio
async def test_buffering_works():
    actionengine._C.save_event_loop_globally(asyncio.get_running_loop())

    server_identity = str(uuid.uuid4())
    client_identity = str(uuid.uuid4())

    action_registry, service, webrtc_server = make_webrtc_server_with_handler(
        handle_connection_for_test_buffering_works,
        server_identity,
        preferred_port=20003,
        run=True,
    )

    client_stream = await asyncio.to_thread(
        actionengine.webrtc.make_webrtc_stream,
        client_identity,
        server_identity,
        SIGNALLING_URL,
    )
    await asyncio.to_thread(client_stream.start)
    await asyncio.to_thread(client_stream.half_close)

    buffered_node_fragments = [make_node_fragment(i) for i in range(3)]
    expected_first_message = actionengine.WireMessage()
    expected_first_message.node_fragments.extend(buffered_node_fragments)

    unbuffered_node_fragments = [make_node_fragment(i) for i in range(3, 6)]
    expected_following_messages = []
    for nf in unbuffered_node_fragments:
        wm = actionengine.WireMessage()
        wm.node_fragments.append(nf)
        expected_following_messages.append(wm)

    message = await asyncio.to_thread(client_stream.receive)
    assert message == expected_first_message

    for expected_message in expected_following_messages:
        message = await asyncio.to_thread(client_stream.receive)
        assert message == expected_message

    message = await asyncio.to_thread(client_stream.receive)
    assert message is None

    webrtc_server.cancel()
    await asyncio.to_thread(webrtc_server.join)


async def handle_connection_for_test_force_flush_works(
        stream: actionengine._C.webrtc.WebRtcWireStream,
        _: actionengine.Session,
        recv_timeout: float = -1.0,
):
    received = await asyncio.to_thread(stream.receive, recv_timeout)
    assert received is None  # half-closed without messages

    with actionengine.buffer_wire_messages(stream) as buffer_context:
        for idx in range(3):
            wire_message = actionengine.WireMessage()
            wire_message.node_fragments.append(make_node_fragment(idx))
            await asyncio.to_thread(stream.send, wire_message)
            if idx == 1:
                await asyncio.to_thread(buffer_context.force_flush)

    await asyncio.to_thread(stream.half_close)


@pytest.mark.asyncio
async def test_force_flush_works():
    actionengine._C.save_event_loop_globally(asyncio.get_running_loop())

    server_identity = str(uuid.uuid4())
    client_identity = str(uuid.uuid4())

    action_registry, service, webrtc_server = make_webrtc_server_with_handler(
        handle_connection_for_test_force_flush_works,
        server_identity,
        preferred_port=20003,
        run=True,
    )

    client_stream = await asyncio.to_thread(
        actionengine.webrtc.make_webrtc_stream,
        client_identity,
        server_identity,
        SIGNALLING_URL,
    )
    await asyncio.to_thread(client_stream.start)
    await asyncio.to_thread(client_stream.half_close)

    first_two_buffered_node_fragments = [
        make_node_fragment(i) for i in range(2)
    ]
    expected_first_message = actionengine.WireMessage()
    expected_first_message.node_fragments.extend(
        first_two_buffered_node_fragments
    )

    third_buffered_node_fragment = make_node_fragment(2)
    expected_second_message = actionengine.WireMessage()
    expected_second_message.node_fragments.append(third_buffered_node_fragment)

    message = await asyncio.to_thread(client_stream.receive)
    assert message == expected_first_message

    message = await asyncio.to_thread(client_stream.receive)
    assert message == expected_second_message

    message = await asyncio.to_thread(client_stream.receive)
    assert message is None

    webrtc_server.cancel()
    await asyncio.to_thread(webrtc_server.join)


async def handle_connection_for_test_half_close_works_with_buffering(
        stream: actionengine._C.webrtc.WebRtcWireStream,
        _: actionengine.Session,
        recv_timeout: float = -1.0,
):
    received = await asyncio.to_thread(stream.receive, recv_timeout)
    assert received is None  # half-closed without messages

    with actionengine.buffer_wire_messages(stream):
        for idx in range(3):
            wire_message = actionengine.WireMessage()
            wire_message.node_fragments.append(make_node_fragment(idx))
            await asyncio.to_thread(stream.send, wire_message)

        # Now half-close while still in buffering context
        await asyncio.to_thread(stream.half_close)


@pytest.mark.asyncio
async def test_half_close_works_with_buffering():
    actionengine._C.save_event_loop_globally(asyncio.get_running_loop())

    server_identity = str(uuid.uuid4())
    client_identity = str(uuid.uuid4())

    action_registry, service, webrtc_server = make_webrtc_server_with_handler(
        handle_connection_for_test_half_close_works_with_buffering,
        server_identity,
        preferred_port=20003,
        run=True,
    )

    client_stream = await asyncio.to_thread(
        actionengine.webrtc.make_webrtc_stream,
        client_identity,
        server_identity,
        SIGNALLING_URL,
    )
    await asyncio.to_thread(client_stream.start)
    await asyncio.to_thread(client_stream.half_close)

    buffered_node_fragments = [make_node_fragment(i) for i in range(3)]
    expected_message = actionengine.WireMessage()
    expected_message.node_fragments.extend(buffered_node_fragments)

    message = await asyncio.to_thread(client_stream.receive)
    assert message == expected_message

    message = await asyncio.to_thread(client_stream.receive)
    assert message is None

    webrtc_server.cancel()
    await asyncio.to_thread(webrtc_server.join)


async def handle_connection_for_test_headers(
        stream: actionengine._C.webrtc.WebRtcWireStream,
        _: actionengine.Session,
        recv_timeout: float = -1.0,
):
    with actionengine.buffer_wire_messages(stream):
        wire_message_1 = actionengine.WireMessage()
        wire_message_1.set_header("issuer", "user123")
        wire_message_1.set_header("priority", bytes("high", "utf-8"))
        await asyncio.to_thread(stream.send, wire_message_1)

        wire_message_2 = actionengine.WireMessage()
        wire_message_2.node_fragments.append(make_node_fragment(0))
        wire_message_2.set_header("secret", b"\x01\x02\x03\x04")
        await asyncio.to_thread(stream.send, wire_message_2)

    await asyncio.to_thread(stream.half_close)


@pytest.mark.asyncio
async def test_wire_message_headers():
    actionengine._C.save_event_loop_globally(asyncio.get_running_loop())

    server_identity = str(uuid.uuid4())
    client_identity = str(uuid.uuid4())

    action_registry, service, webrtc_server = make_webrtc_server_with_handler(
        handle_connection_for_test_headers,
        server_identity,
        preferred_port=20003,
        run=True,
    )
    client_stream = await asyncio.to_thread(
        actionengine.webrtc.make_webrtc_stream,
        client_identity,
        server_identity,
        SIGNALLING_URL,
    )
    await asyncio.to_thread(client_stream.start)
    await asyncio.to_thread(client_stream.half_close)

    wire_message: actionengine.WireMessage = await asyncio.to_thread(
        client_stream.receive
    )
    assert set(wire_message.headers()) == {"issuer", "priority", "secret"}
    assert wire_message.get_header("issuer", decode=True) == "user123"
    assert wire_message.get_header("priority", decode=True) == "high"
    assert wire_message.get_header("secret") == b"\x01\x02\x03\x04"

    webrtc_server.cancel()
    await asyncio.to_thread(webrtc_server.join)
