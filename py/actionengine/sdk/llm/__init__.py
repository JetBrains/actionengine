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

import enum

from . import generate_content
from . import generate_content_handler

from actionengine import data

make_scoped_header_key = data.make_scoped_header_key


class LLMHeaders(enum.StrEnum):
    ALLOWED_TOOLS = make_scoped_header_key("llm-allowed-tools")
    PROVIDER = make_scoped_header_key("llm-provider")
    MODEL = make_scoped_header_key("llm-model")
    EXTRA_CONFIG = make_scoped_header_key("llm-extra-config")
    API_KEY = make_scoped_header_key("llm-api-key")
