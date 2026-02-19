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

import uuid

import actionengine
import pytest

ECHO_SCHEMA = actionengine.ActionSchema(
    name="echo_with_headers",
    inputs=[("input_text", "text/plain")],
    outputs=[("output_text", "text/plain")],
    description="An action that echoes the input text along with headers.",
)


@pytest.mark.asyncio
async def test_action_headers():
    action = actionengine.Action(ECHO_SCHEMA, str(uuid.uuid4()))
    action.set_header("issuer", "user123")
    action.set_header("priority", bytes("high", "utf-8"))
    action.set_header("secret", b"\x01\x02\x03\x04")

    issuer_bytes = action.get_header("issuer")
    assert issuer_bytes == b"user123"
    issuer_decoded = action.get_header("issuer", decode=True)
    assert issuer_decoded == "user123"

    priority = action.get_header("priority")
    assert priority == b"high"
    priority_decoded = action.get_header("priority", decode=True)
    assert priority_decoded == "high"

    secret = action.get_header("secret")
    assert secret == b"\x01\x02\x03\x04"

    assert action.get_header("nonexistent_key") is None

    all_headers = set(action.headers())
    assert all_headers == {"issuer", "priority", "secret"}


@pytest.mark.asyncio
async def test_action_message_headers():
    action = actionengine.Action(ECHO_SCHEMA, str(uuid.uuid4()))
    action.set_header("issuer", "user123")
    action.set_header("priority", bytes("high", "utf-8"))
    action.set_header("secret", b"\x01\x02\x03\x04")

    action_message = action.get_action_message()

    issuer_bytes = action_message.get_header("issuer")
    assert issuer_bytes == b"user123"
    issuer_decoded = action_message.get_header("issuer", decode=True)
    assert issuer_decoded == "user123"

    priority = action_message.get_header("priority")
    assert priority == b"high"
    priority_decoded = action_message.get_header("priority", decode=True)
    assert priority_decoded == "high"

    secret = action_message.get_header("secret")
    assert secret == b"\x01\x02\x03\x04"

    assert action_message.get_header("nonexistent_key") is None

    all_headers = set(action_message.headers())
    assert all_headers == {"issuer", "priority", "secret"}


@pytest.mark.asyncio
async def test_wire_message_headers():
    wire_message = actionengine.WireMessage()
    wire_message.set_header("wire_header", "wire_value")
    wire_message.set_header("wire_secret", b"\x05\x06\x07\x08")

    wire_header_bytes = wire_message.get_header("wire_header")
    assert wire_header_bytes == b"wire_value"
    wire_header_decoded = wire_message.get_header("wire_header", decode=True)
    assert wire_header_decoded == "wire_value"
    wire_secret = wire_message.get_header("wire_secret")
    assert wire_secret == b"\x05\x06\x07\x08"

    assert wire_message.get_header("nonexistent_key") is None

    all_headers = set(wire_message.headers())
    assert all_headers == {"wire_header", "wire_secret"}
