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

"""A Pythonic wrapper for the raw pybind11 Service bindings."""

import asyncio
import inspect
from typing import Callable
from typing import Coroutine

from actionengine import _C
from actionengine import actions
from actionengine import session as eg_session
from actionengine import stream as eg_stream
from actionengine import utils

Session = eg_session.Session
WireStream = eg_stream.WireStream

AsyncConnectionHandler = Callable[
    [_C.service.WireStream, Session, float], Coroutine[None, None, None]
]
SyncConnectionHandler = Callable[[_C.service.WireStream, Session, float], None]
ConnectionHandler = SyncConnectionHandler | AsyncConnectionHandler


def wrap_async_handler(
        handler: AsyncConnectionHandler,
) -> SyncConnectionHandler:
    """Wraps the given handler to run in the event loop."""
    loop = asyncio.get_running_loop()

    def sync_handler(
            stream: _C.service.WireStream,
            session: Session,
            recv_timeout: float = -1.0,
    ) -> None:
        result = asyncio.run_coroutine_threadsafe(
            handler(
                stream,
                utils.wrap_pybind_object(Session, session),
                recv_timeout,
            ),
            loop,
        )
        result.result()

    return sync_handler


def wrap_sync_handler(handler: SyncConnectionHandler) -> SyncConnectionHandler:
    def sync_handler(
            stream: _C.service.WireStream,
            session: Session,
            recv_timeout: float = -1.0,
    ) -> None:
        return handler(
            stream,
            utils.wrap_pybind_object(Session, session),
            recv_timeout,
        )

    return sync_handler


def wrap_handler(handler: ConnectionHandler | None) -> ConnectionHandler | None:
    if handler is None:
        return handler
    if inspect.iscoroutinefunction(handler):
        return wrap_async_handler(handler)
    else:
        return wrap_sync_handler(handler)


class Service(_C.service.Service):
    """A Pythonic wrapper for the raw pybind11 Service bindings."""

    def __init__(
            self,
            action_registry: actions.ActionRegistry,
            connection_handler: ConnectionHandler | None = None,
    ):
        super().__init__(action_registry, wrap_handler(connection_handler))
