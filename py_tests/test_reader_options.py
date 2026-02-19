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

import actionengine
import pytest


@pytest.mark.asyncio
async def test_reader_options_remove_chunks():
    node = actionengine.AsyncNode("test_node").set_reader_options(
        remove_chunks=False
    )
    await node.put("chunk 0")
    await node.put_and_finalize("chunk 1")

    assert await node.next() == "chunk 0"
    assert await node.next() == "chunk 1"
    assert await node.next() is None

    # Verify that the chunks are still in the chunk store after reading:
    chunk_store = node.get_chunk_store()
    assert len(chunk_store) == 2
    assert 0 in chunk_store
    assert 1 in chunk_store
    assert chunk_store.get(0).data == b"chunk 0"
    assert chunk_store.get(1).data == b"chunk 1"


@pytest.mark.asyncio
async def test_reader_options_read_in_order():
    node = actionengine.AsyncNode("test_node").set_reader_options(
        ordered=True, remove_chunks=False
    )
    await node.put("chunk 2", seq=2, final=True)
    await node.put("chunk 0", seq=0)
    await node.put("chunk 1", seq=1)

    assert await node.next() == "chunk 0"
    assert await node.next() == "chunk 1"
    assert await node.next() == "chunk 2"
    assert await node.next() is None

    chunk_store = node.get_chunk_store()
    assert chunk_store.get_final_seq() == 2

    node = actionengine.AsyncNode("test_node_unordered").set_reader_options(
        ordered=False, remove_chunks=False
    )
    await node.put("chunk 2", seq=2, final=True)
    await node.put("chunk 0", seq=0)
    await node.put("chunk 1", seq=1)

    assert await node.next() == "chunk 2"
    assert await node.next() == "chunk 0"
    assert await node.next() == "chunk 1"

    chunk_store = node.get_chunk_store()
    assert chunk_store.get_final_seq() == 2


@pytest.mark.asyncio
async def test_reader_options_deserialise_automatically():
    # Deserialize automatically enabled:
    node = actionengine.AsyncNode("test_node")
    texts = ["0", "1", "2", "3", "4"]
    for text in texts:
        await node.put(text)
    await node.finalize()

    with node.deserialize_automatically(True):
        for expected_text in texts:
            received_text = await node.next()
            assert isinstance(
                received_text, str
            ), "Expected deserialized string"
            assert received_text == expected_text

    # Deserialize automatically disabled:
    node = actionengine.AsyncNode("test_node_2")
    for text in texts:
        await node.put(text)
    await node.finalize()
    with node.deserialize_automatically(False):
        for expected_text in texts:
            received_chunk = await node.next()
            assert isinstance(
                received_chunk, actionengine.Chunk
            ), "Expected raw Chunk"
            assert received_chunk.data == bytes(expected_text, "utf-8")
