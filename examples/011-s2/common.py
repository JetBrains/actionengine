import asyncio
import os

import actionengine
from redis import asyncio as aioredis

ECHO_SCHEMA = actionengine.ActionSchema(
    name="echo",
    description="An action that echoes input messages to output.",
    inputs=[("input", "text/plain")],
    outputs=[("output", "text/plain")],
)


def get_aioredis_client() -> aioredis.Redis:
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    redis = aioredis.from_url(
        f"redis://{redis_host}:{redis_port}",
        decode_responses=False,
    )
    return redis


def get_actionengine_redis_client():
    if not hasattr(get_actionengine_redis_client, "client"):
        get_actionengine_redis_client.client = actionengine.redis.Redis.connect(
            "localhost"
        )
    return get_actionengine_redis_client.client


def make_redis_chunk_store(node_id: str) -> actionengine.redis.ChunkStore:
    redis_client = get_actionengine_redis_client()
    store = actionengine.redis.ChunkStore(redis_client, node_id, -1)  # No TTL
    return store


def get_queue_key_for_action_name(action_name: str) -> str:
    return f"actionengine:wq:{action_name}"


def get_action_name_from_queue_key(queue_key: str | bytes):
    if isinstance(queue_key, bytes):
        queue_key = queue_key.decode("utf-8")

    if not queue_key.startswith("actionengine:wq:"):
        raise ValueError(f"Invalid queue key format: {queue_key}")

    return queue_key[len("actionengine:wq:") :]


async def post_to_workers(action: actionengine.Action):
    action.bind_node_map(actionengine.NodeMap(make_redis_chunk_store))

    aio_redis = get_aioredis_client()

    wire_message = actionengine.WireMessage()
    wire_message.actions.append(action.get_action_message())

    await aio_redis.rpush(
        get_queue_key_for_action_name(action.get_schema().name),
        await asyncio.to_thread(wire_message.pack_msgpack),
    )
    await action.call()

    return action
