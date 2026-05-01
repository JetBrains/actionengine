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

from typing import Literal

from actionengine.actions import ActionSchema
from pydantic import BaseModel, Field


class CreateResponseConfig(BaseModel):
    max_output_tokens: int = Field(
        default=32768,
        description="Maximum number of output tokens to generate.",
    )
    model: str = Field(
        default="gpt-5.5",
        description="OpenAI model to use for generation.",
    )
    reasoning_effort: (
        Literal["none", "low", "medium", "high", "xhigh"] | None
    ) = Field(
        default="none",
        description="Reasoning effort for models that support reasoning.",
    )


GENERATE_CONTENT_OPENAI_SCHEMA = ActionSchema(
    name="generate_content_openai",
    inputs=[
        ("api_key", "text/plain"),
        ("chat_input", "text/plain"),
        ("system_instructions", "text/plain"),
        ("interaction_token", "text/plain"),
        ("config", CreateResponseConfig),
        ("tools", "application/json"),
    ],
    outputs=[
        ("output", "text/plain"),
        ("thoughts", "text/plain"),
        ("new_interaction_token", "text/plain"),
    ],
)
