import asyncio
import os

import actionengine

from common import (
    get_aioredis_client,
    get_queue_key_for_action_name,
    make_redis_chunk_store,
    ECHO_SCHEMA,
)


async def run_echo(action: actionengine.Action):
    action["input"].set_reader_options(timeout=10.0, ordered=True)
    async for message in action["input"]:
        await action["output"].put(message)
        print(f"[echo] Echoing `{message}`")

    worker_name = os.environ.get("WORKER_NAME", "unknown")
    await action["output"].put(f" (worker: {worker_name})")
    await action["output"].finalize()
    print(f"[echo] Worker {worker_name} finished.")


async def listen_and_execute_actions(
    action_registry: actionengine.ActionRegistry,
):
    queues = [
        get_queue_key_for_action_name(action_name)
        for action_name in action_registry.list_registered_actions()
    ]

    aio_redis = get_aioredis_client()
    while True:
        try:
            queue_key, message = await aio_redis.blpop(queues)
        except asyncio.CancelledError:
            break

        message = await asyncio.to_thread(
            actionengine.WireMessage.from_msgpack, message
        )

        for action_message in message.actions:
            action_registry.make_action(
                action_message.name,
                action_message.id,
                node_map=actionengine.NodeMap(make_redis_chunk_store),
            ).run()


async def main():
    registry = actionengine.ActionRegistry()
    registry.register("echo", ECHO_SCHEMA, run_echo)

    await listen_and_execute_actions(registry)


def sync_main():
    settings = actionengine.get_global_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_remove_read_chunks = False

    asyncio.run(main())


if __name__ == "__main__":
    sync_main()
