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

import datetime

import actionengine
import pytest
from pydantic import BaseModel


@pytest.mark.asyncio
async def test_basic_metadata_ops():
    default_metadata = actionengine.ChunkMetadata()
    assert default_metadata.timestamp is None
    # default is bytes
    assert default_metadata.mimetype == "application/octet-stream"
    assert not default_metadata.attributes

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    metadata = actionengine.ChunkMetadata(mimetype="text/plain")
    metadata.timestamp = now
    metadata.set_attribute("key1", "value1")
    metadata.set_attribute("key2", "value2")

    assert metadata.timestamp == now
    assert metadata.mimetype == "text/plain"
    assert metadata.get_attribute("key1") == b"value1"
    assert metadata.get_attribute("key2") == b"value2"
    with pytest.raises(RuntimeError):
        metadata.get_attribute("")
    with pytest.raises(RuntimeError):
        metadata.get_attribute("nonexistent_key")

    all_attributes = dict(metadata.attributes)
    assert all_attributes == {"key1": b"value1", "key2": b"value2"}


@pytest.mark.asyncio
async def test_basic_chunk_ops():
    default_chunk = actionengine.Chunk()
    assert default_chunk.metadata is None
    assert default_chunk.data == b""
    assert default_chunk.ref == ""

    data_chunk = actionengine.Chunk()
    data_chunk.data = b"sample data"
    assert data_chunk.data == b"sample data"
    assert data_chunk.metadata is None
    assert data_chunk.ref == ""

    ref_chunk = actionengine.Chunk()
    ref_chunk.ref = "test_ref_123"
    assert ref_chunk.ref == "test_ref_123"
    assert ref_chunk.data == b""
    assert ref_chunk.metadata is None

    # data property should be equally able to accept str and bytes
    chunk = actionengine.Chunk()
    chunk.data = b"hello world"
    assert chunk.data == b"hello world"
    chunk.data = "hello text"
    assert chunk.data == b"hello text"

    class SampleModel(BaseModel):
        field1: str
        field2: int

    # data property should be able to accept any object registered with
    # the global serializer
    pydantic_model_instance = SampleModel(field1="test", field2=42)
    chunk.metadata = actionengine.ChunkMetadata(mimetype="__BaseModel__")
    chunk.data = pydantic_model_instance
    assert chunk.deserialize() == pydantic_model_instance

    # test make_from utility: serialization should be automatic,
    # mimetype should be set appropriately
    chunk_from_util = actionengine.Chunk.make_from(pydantic_model_instance)
    assert chunk_from_util.metadata.mimetype == "__BaseModel__"
    assert chunk_from_util.deserialize() == pydantic_model_instance

    # test make_from correctly sets mimetype even for bytes input
    byte_data = b"byte data example"
    chunk_from_bytes = actionengine.Chunk.make_from(byte_data)
    assert chunk_from_bytes.metadata.mimetype == "application/octet-stream"
    assert chunk_from_bytes.data == byte_data


@pytest.mark.asyncio
async def test_node_ref_ops():
    default_node_ref = actionengine.data.NodeRef()
    assert default_node_ref.id == ""
    assert default_node_ref.offset == 0
    assert default_node_ref.length is None

    # test initialization with parameters
    node_ref = actionengine.data.NodeRef(id="node_123", offset=10, length=50)
    assert node_ref.id == "node_123"
    assert node_ref.offset == 10
    assert node_ref.length == 50

    # test setting properties
    node_ref.id = "node_456"
    node_ref.offset = 20
    node_ref.length = 100
    assert node_ref.id == "node_456"
    assert node_ref.offset == 20
    assert node_ref.length == 100

    # test length can be set to None
    node_ref.length = None
    assert node_ref.length is None


@pytest.mark.asyncio
async def test_node_fragment_ops():
    default_fragment = actionengine.NodeFragment()
    assert default_fragment.id == ""
    assert default_fragment.seq is None
    assert not default_fragment.continued
    assert default_fragment.chunk is not None
    with pytest.raises(RuntimeError):
        _ = default_fragment.node_ref

    # test initialization with parameters
    fragment = actionengine.NodeFragment(
        id="fragment_1",
        seq=1,
        continued=True,
    )
    fragment.chunk = actionengine.Chunk()
    assert fragment.id == "fragment_1"
    assert fragment.seq == 1
    assert fragment.continued is True
    assert fragment.chunk == actionengine.Chunk()
    with pytest.raises(RuntimeError):
        _ = fragment.node_ref

    # test setting properties
    fragment.id = "fragment_2"
    fragment.seq = 2
    fragment.continued = False
    another_chunk = actionengine.Chunk()
    another_chunk.data = b"new data"
    fragment.chunk = another_chunk
    assert fragment.id == "fragment_2"
    assert fragment.seq == 2
    assert fragment.continued is False
    assert fragment.chunk == another_chunk
    with pytest.raises(RuntimeError):
        _ = fragment.node_ref

    # can set node_ref, and that prevents access to chunk
    fragment.node_ref = actionengine.data.NodeRef(
        id="node_789", offset=0, length=10
    )
    assert fragment.node_ref.id == "node_789"
    assert fragment.node_ref.offset == 0
    assert fragment.node_ref.length == 10
    with pytest.raises(RuntimeError):
        _ = fragment.chunk
