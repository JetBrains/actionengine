import datetime
from typing import ClassVar

import actionengine
from actionengine import pydantic_helpers
from pydantic import Field


def get_ae_redis_client(host: str = "localhost"):
    if not hasattr(get_ae_redis_client, "client"):
        get_ae_redis_client.client = actionengine.redis.Redis.connect(host)
    return get_ae_redis_client.client


def make_redis_chunk_store_factory(client: actionengine.redis.Redis):
    def make_chunk_store(node_id: str) -> actionengine.redis.ChunkStore:
        return actionengine.redis.ChunkStore(client, node_id, ttl=-1)

    return make_chunk_store


class Message(pydantic_helpers.ActSchema):
    _act_schema_name: ClassVar[str] = "Message"

    text: str
    timestamp: datetime.datetime = Field(
        default_factory=datetime.datetime.utcnow
    )


def setup_action_engine():
    settings = actionengine.get_global_settings()
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True
    settings.readers_deserialise_automatically = True

    # a temporary hack to get the schema registered for serialization
    actionengine.to_chunk(Message(text="Hello world!"))
