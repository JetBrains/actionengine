import argparse
import asyncio

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

    message: Message  # Pydantic model, has fields "timestamp" and "text"
    async for message in node_map[args.node_id].set_reader_options(
        timeout=args.timeout
    ):
        try:
            print(f"[{message.timestamp}]: {message.text}")
        except Exception as exc:
            print(f"Error reading message: {exc}")
            break

    print("Stream ended.")


def sync_main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", type=str, default="localhost")
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--node-id", type=str, default="test-node")

    asyncio.run(main(parser.parse_args()))


if __name__ == "__main__":
    sync_main()
