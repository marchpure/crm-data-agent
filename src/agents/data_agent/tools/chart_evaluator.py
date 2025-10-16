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
"""Chart Evaluator Sub-tool"""

from google.adk.tools import ToolContext
from google.genai.types import Content, GenerateContentConfig, Part, SafetySetting

from pydantic import BaseModel

from .utils import get_volcengine_llm_client
from prompts.chart_evaluator import prompt as chart_evaluator_prompt


import os

MODEL_ID = os.environ.get("VE_LLM_MODEL_ID", "doubao-pro-32k")
CHART_EVALUATOR_MODEL_ID = MODEL_ID

class EvaluationResult(BaseModel):
    is_good: bool
    reason: str


def evaluate_chart(png_image: bytes,
                   chart_json_text: str,
                   question: str,
                   data_row_count: int,
                   tool_context: ToolContext) -> EvaluationResult:
    """
    This is an experienced Business Intelligence UX designer.
    They look at a chart or a dashboard, and can tell if it the right one for the question.

    Parameters:
    * png_image (str) - png image of the chart or a dashboard
    * question (str) - question this chart is supposed to answer

    """

    prompt = chart_evaluator_prompt.format(data_row_count=data_row_count,
                                           chart_json=chart_json_text,
                                            question=question)

    import base64
    import json

    image_b64 = base64.b64encode(png_image).decode("utf-8")

    system_prompt = """
You are an experienced Business Intelligence UX designer.
You can look at a chart or a dashboard, and tell if it the right one for the question.
""".strip()

    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {
                    "type": "image",
                    "image_url": {"url": f"data:image/png;base64,{image_b64}"},
                },
            ],
        },
    ]

    completion = get_volcengine_llm_client().chat.completions.create(
        model=CHART_EVALUATOR_MODEL_ID,
        messages=messages,
        temperature=0.1,
        max_tokens=4096,
        response_format={"type": "json_object"},
    )
    eval_json_text = completion.choices[0].message.content

    return EvaluationResult.model_validate_json(eval_json_text)
