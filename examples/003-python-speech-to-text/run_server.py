import asyncio
import uuid

import actionengine

from stt.actions import make_action_registry
from stt.serialisation import register_stt_serialisers


SIGNALLING_URL = "wss://actionengine.dev:19001"


def setup_action_engine():
    register_stt_serialisers()

    settings = actionengine.get_global_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True


async def sleep_forever():
    while True:
        await asyncio.sleep(1)


async def main():
    setup_action_engine()

    action_registry = make_action_registry()
    service = actionengine.Service(action_registry)

    server_identity = "sttdemo"

    rtc_config = actionengine.webrtc.RtcConfig()
    rtc_config.preferred_port_range = (20003, 20003)
    webrtc_server = actionengine.webrtc.WebRtcServer.create(
        service,
        "0.0.0.0",
        server_identity,
        SIGNALLING_URL,
        rtc_config,
    )
    webrtc_server.run()
    print(
        f"Action Engine WebRTC server running with identity {server_identity}"
    )

    try:
        await sleep_forever()
    except asyncio.CancelledError:
        print("Main task cancelled, shutting down.")
    finally:
        print("Shutting down Action Engine server.")
        webrtc_server.cancel()
        await asyncio.to_thread(webrtc_server.join)


def sync_main():
    return asyncio.run(main())


if __name__ == "__main__":
    sync_main()
