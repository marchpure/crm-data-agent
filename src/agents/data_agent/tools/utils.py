'''
Author: haoxingjun
Date: 2025-10-14 14:30:34
Email: haoxingjun@bytedance.com
LastEditors: haoxingjun
LastEditTime: 2025-10-16 05:15:08
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
"""Utils for agents - Adapted for Volcengine"""

import os
from threading import Lock
from openai import OpenAI

_lock = Lock()
_llm_client = None

def get_volcengine_llm_client() -> OpenAI:
    """Initializes and returns a singleton OpenAI client configured for Volcengine Ark."""
    global _llm_client
    if _llm_client:
        return _llm_client
    with _lock:
        if _llm_client:
            return _llm_client

        try:
            _llm_client = OpenAI(
                api_key=os.environ["ARK_API_KEY"],
                base_url=os.environ.get("ARK_BASE_URL", "https://ark.cn-beijing.volces.com/api/v3")
            )
            print("Initialized OpenAI client for Volcengine Ark.")

        except ImportError:
            raise ImportError("OpenAI SDK not installed. Please run 'pip install --upgrade \"openai>=1.0\"' to proceed.")
        except KeyError as e:
            raise EnvironmentError(f"Missing required environment variable for OpenAI client: {e}")

    return _llm_client

def get_volcengine_model(model_id: str):
    # This function is now a simple pass-through, as the model is specified in each API call.
    # It's kept for compatibility with the existing agent structure.
    return model_id
    
    # Assuming VEADK has a model wrapper similar to ADK's Gemini
    # from veadk.models import VolcengineLlm 
    # return VolcengineLlm(model=model_id, client=get_volcengine_llm_client())
    
    # For now, just return the model_id, as the agent might handle it.
    return model_id

def get_shared_lock() -> Lock:
    return _lock