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

from typing import TypeVar

from actionengine import _C

T = TypeVar("T")

GlobalSettings = _C.GlobalSettings


def get_global_settings() -> GlobalSettings:
    """Returns the global settings for actionengine."""
    return _C.get_global_settings()


# legacy alias
def get_global_act_settings() -> GlobalSettings:
    return get_global_settings()


def global_setting_if_none(value: T | None, setting: str) -> T:
    """Returns the globally set value if the value is None, otherwise returns the provided value."""
    settings = get_global_settings()
    if value is None:
        return getattr(settings, setting)
    return value
