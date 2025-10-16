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
"""Agent Runner App - FastAPI service - Adapted for Volcengine"""

import logging
import os
from pathlib import Path
import sys

# Placeholder for Volcengine Artifact Service
# from veadk.artifacts import TosArtifactService
# Using ADK's InMemory service as a fallback placeholder
from google.adk.artifacts import InMemoryArtifactService
from fast_api_app import get_fast_api_app

sys.path.append(str(Path(__file__).parent.parent))
from shared.config_env import prepare_environment

#################### Initialization ####################
logging.getLogger().setLevel(logging.INFO)
os.environ["AGENT_DIR"] = str(Path(__file__).parent.parent /
                                        "agents" /
                                        "data_agent")
prepare_environment()
########################################################

# This should be replaced with the VEADK equivalent, e.g., TosArtifactService
# artifact_service = TosArtifactService(
#     bucket_name=os.environ["VE_TOS_BUCKET"]
# )
# Using an in-memory service as a temporary placeholder.
artifact_service = InMemoryArtifactService()

api_app = get_fast_api_app(
    agent_dir=os.environ["AGENT_DIR"],
    trace_to_cloud=False,
    artifact_service=artifact_service
)


