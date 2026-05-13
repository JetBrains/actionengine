import asyncio

import actionengine

from common import post_to_workers, ECHO_SCHEMA


async def main():
    for idx in range(10):
        # Create an action, run .call() and post an ActionMessage to its
        # respective worker queue
        echo = await post_to_workers(
            actionengine.Action.from_schema(ECHO_SCHEMA)
        )

        await echo["input"].put_and_finalize(f"Hello, Action Engine! {idx + 1}")

        async for piece in echo["output"]:
            print(piece, end="")
        print()

        # Wait for the action's completion and status
        await echo.wait_until_complete()


def sync_main():
    settings = actionengine.get_global_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_remove_read_chunks = True

    asyncio.run(main())


if __name__ == "__main__":
    sync_main()
