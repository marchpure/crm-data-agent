'''
Author: haoxingjun
Date: 2025-10-14 14:30:34
Email: haoxingjun@bytedance.com
LastEditors: haoxingjun
LastEditTime: 2025-10-16 04:37:08
Description: file information
Company: ByteDance
'''
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Business Analyst Agent"""

import uuid

from veadk import Agent
from google.adk.agents.callback_context import CallbackContext
from google.adk.models import LlmResponse
from google.adk.planners import BuiltInPlanner

from google.genai.types import (
    GenerateContentConfig,
    Part,
    SafetySetting,
    ThinkingConfig
)

from prompts.crm_business_analyst import (system_instruction
                                          as crm_business_analyst_instruction)
from tools.utils import get_volcengine_model


BUSINESS_ANALYST_AGENT_MODEL_ID = "gemini-2.5-pro" # "gemini-2.5-pro-preview-05-06"


async def after_model_callback(callback_context: CallbackContext,
                          llm_response: LlmResponse) -> LlmResponse | None:
    if not llm_response.content or not llm_response.content.parts:
        return
    for p in llm_response.content.parts:
        if p.text and p.text.strip():
            await callback_context.save_artifact(
                f"analysis_{uuid.uuid4().hex}.md",
                Part.from_bytes(
                    mime_type="text/markdown",
                    data=p.text.encode("utf-8")
                )
            )


crm_business_analyst_agent = Agent(
    model=get_volcengine_model(BUSINESS_ANALYST_AGENT_MODEL_ID),
    name="crm_business_analyst",
    description="""
        This is your Senior Business Analyst.

        They can analyze your questions about business
        no matter what form these questions are in.

        Questions may be different:
            - Directly linked to business data (e.g. "Revenue by country")
            - Open to interpretation (e.g. "Who are my best customers").

        They figure out what metrics, dimensions and KPIs
        could be used to answer the question.

        They may offer a few options.
        """,
    instruction=crm_business_analyst_instruction,
    after_model_callback=after_model_callback
)
