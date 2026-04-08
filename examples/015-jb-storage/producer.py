import argparse
import asyncio
import uuid

import actionengine

from common import (
    get_ae_redis_client,
    make_redis_chunk_store_factory,
    Message,
    setup_action_engine,
)


async def main(args: argparse.Namespace):
    setup_action_engine()

    redis_client = get_ae_redis_client(args.endpoint)
    node_map = actionengine.NodeMap(
        make_redis_chunk_store_factory(redis_client)
    )

    node_id = str(uuid.uuid4())
    print(
        f"Welcome to the message queue demo. Run the consumer in another "
        f"terminal as follows:\n"
        f"  python consumer.py --endpoint {args.endpoint} --node-id {node_id}\n\n"
        f"From now on, you can enter messages. Each message will be enqueued for "
        f"the consumer to read asynchronously. Press Ctrl+C or type '/quit' to exit.\n"
    )

    node = node_map[node_id]

    try:
        while True:
            try:
                text = await asyncio.to_thread(input, "> ")
                if text.lower() == "/quit":
                    break

                # Message is a Pydantic model that has fields "timestamp" and "text"
                await node.put(Message(text=text))
            except KeyboardInterrupt:
                break
    finally:
        await node.finalize()


def sync_main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", type=str, default="localhost")

    asyncio.run(main(parser.parse_args()))


if __name__ == "__main__":
    sync_main()
