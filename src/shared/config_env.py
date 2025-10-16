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
"""Configuration Environment Variables Loader"""

import os
from dotenv import load_dotenv, dotenv_values
from pathlib import Path
import logging
import sys


_env_requirements = {
    # Volcengine Credentials
    "VOLCENGINE_AK": None,
    "VOLCENGINE_SK": None,
    "VOLCENGINE_REGION": "cn-beijing",

    # EMR Serverless Presto Configuration
    "EMR_CATALOG": "hive_catalog",
    "EMR_DATABASE": "test_db",

    # Volcengine Large Language Model Configuration
    "VE_LLM_MODEL_ID": None,

    # Volcengine Services Configuration
    "VE_TOS_BUCKET": None,


    # Metadata Configuration
    "SFDC_METADATA_FILE": "sfdc_metadata.json",
}
_prepared = False

def _get_dotenv_file() -> str:
    dotenv_path = Path(__file__).parent.parent / ".env"
    if not dotenv_path.exists():
        logging.warning(f"{dotenv_path} not found.")
        return ""
    return str(dotenv_path)

def get_env_values() -> dict:
    env_file = _get_dotenv_file()
    if not env_file:
        logging.warning(".env file not found. Trying to re-construct from values.")
        env_dict = {}
        for name in _env_requirements:
            env_dict[name] = os.environ.get(name, None)
        return env_dict
    values = dotenv_values(_get_dotenv_file()).copy()
    for v in values:
        if v in os.environ:
            values[v] = os.environ[v]
    return values


def prepare_environment():
    global _prepared
    if _prepared:
        return
    env_file = _get_dotenv_file()
    if not env_file:
        logging.warning(".env file not found.")
    else:
        load_dotenv(dotenv_path=_get_dotenv_file(), override=True)

    for name, val in _env_requirements.items():
        if name in os.environ and len(os.environ[name].strip()) > 0:
            continue
        if val is None or val.strip() == "":
            logging.error((f"{name} environment variable must be set "
                          "(check .env file)."))
            sys.exit(1)
        elif val.startswith("$"):
            ref_name = val[1:]
            os.environ[name] = os.environ[ref_name]
        else:
            os.environ[name] = val

    # Removed Google Cloud AI Platform initialization
    # from google.cloud.aiplatform import init
    # init(location="global")

    _prepared = True
