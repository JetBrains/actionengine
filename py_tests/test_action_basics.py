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

import actionengine
import pytest


async def run_echo(action: actionengine.Action):
    print("Running echo", flush=True)
    async for chunk in action["input"]:
        await action["output"].put(chunk)
    await action["output"].finalize()
    print("Echo complete", flush=True)


ECHO_SCHEMA = actionengine.ActionSchema(
    name="echo",
    inputs=[("input", "text/plain")],
    outputs=[("output", "text/plain")],
    description="An action that echoes input to output.",
)


def make_action_registry():
    registry = actionengine.ActionRegistry()
    registry.register("echo", ECHO_SCHEMA, run_echo)
    return registry


@pytest.mark.asyncio
async def test_action_runs():
    registry = make_action_registry()
    node_map = actionengine.NodeMap()

    echo = registry.make_action("echo", node_map=node_map).run_in_background()
    await echo["input"].put_and_finalize("Hello!")

    received = await echo["output"].consume(allow_none=True)
    assert received == "Hello!"

    print(f"Received: {received}", flush=True)

    await echo.wait_until_complete()
    print("Echo complete outside", flush=True)


@pytest.mark.asyncio
async def test_chunk_iteration():
    actionengine._C.save_event_loop_globally(asyncio.get_running_loop())
    for _ in range(1000):
        node = actionengine.AsyncNode("test")

        async def produce():
            for word in ("Hello, ", "world!"):
                await node.put(word)
            await node.finalize()

        async def consume():
            assert await node.next() == "Hello, "
            assert await node.next() == "world!"
            assert await node.next() is None

        await asyncio.gather(
            produce(),
            consume(),
        )


@pytest.mark.asyncio
async def test_node_fragment_iteration():
    actionengine._C.save_event_loop_globally(asyncio.get_running_loop())
    for _ in range(1000):
        node = actionengine.AsyncNode("test")

        async def produce():
            for word in ("Hello, ", "world!"):
                await node.put(word)
            await node.finalize()

        async def consume():
            assert (await node.next_fragment()).chunk.data.decode() == "Hello, "
            assert (await node.next_fragment()).chunk.data.decode() == "world!"
            assert await node.next_fragment() is None

        await asyncio.gather(
            produce(),
            consume(),
        )
