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
"""Data Engineer Agent - Adapted for Volcengine EMR Serverless Presto"""

from functools import cache
import json
import os
from pathlib import Path
import uuid
from typing import Tuple

from pydantic import BaseModel

# Volcengine EMR Serverless Presto related imports
from serverless.client import ServerlessClient
from serverless.auth import StaticCredentials
from serverless.task import SQLTask
from serverless.exceptions import QuerySdkError

# Volcengine LLM related imports (assuming a similar structure to Gemini)
# This will be replaced with the actual Volcengine LLM SDK
from volcengine.maas.exception import MaasException
from google.adk.tools import ToolContext

from .utils import get_volcengine_llm_client
from prompts.data_engineer import (system_instruction
                                   as data_engineer_instruction,
                                   prompt as data_engineer_prompt)
from prompts.sql_correction import (instruction as sql_correction_instruction,
                                    prompt as sql_correction_prompt)

# These will be replaced with Volcengine model IDs from environment variables
MODEL_ID = os.environ.get("VE_LLM_MODEL_ID", "doubao-seed-1-6-250615")
DATA_ENGINEER_AGENT_MODEL_ID = MODEL_ID
SQL_VALIDATOR_MODEL_ID = MODEL_ID
_DEFAULT_METADATA_FILE = "sfdc_metadata.json"

@cache
def _init_environment():
    global _access_key, _secret_key, _region, _catalog, _database
    global _sfdc_metadata, _sfdc_metadata_dict

    # Load Volcengine and EMR Presto credentials from environment
    _access_key = os.environ["VOLCENGINE_AK"]
    _secret_key = os.environ["VOLCENGINE_SK"]
    _region = os.environ["VOLCENGINE_REGION"]
    _catalog = os.environ["EMR_CATALOG"]
    _database = os.environ["EMR_DATABASE"]

    _sfdc_metadata_path = os.environ.get("SFDC_METADATA_FILE",
                                        _DEFAULT_METADATA_FILE)
    if not Path(_sfdc_metadata_path).exists():
        if "/" not in _sfdc_metadata_path:
            _sfdc_metadata_path = str(Path(__file__).parent.parent /
                                    _sfdc_metadata_path)

    _sfdc_metadata = Path(_sfdc_metadata_path).read_text(encoding="utf-8")
    _sfdc_metadata_dict = json.loads(_sfdc_metadata)

    # Metadata enhancement logic can remain, as it's not tied to a specific DB
    # The part that connects to the DB to verify tables can be adapted
    _final_dict = {}
    client = ServerlessClient(credentials=StaticCredentials(_access_key, _secret_key),
                              region=_region, endpoint='open.volcengineapi.com', service='emr_serverless')

    try:
        # To list tables, we can run "SHOW TABLES"
        query = f'set tqs.query.engine.type = presto; SHOW TABLES FROM "{_catalog}"."{_database}"'
        job = client.execute(task=SQLTask(name='list_tables_for_metadata', query=query, conf={"tqs.query.engine.type": "presto"}), is_sync=True)

        if job.is_success():
            tables = job.get_result()
            for row in tables:
                table_name = row[0]
                if table_name in _sfdc_metadata_dict:
                    table_dict = _sfdc_metadata_dict[table_name]
                    _final_dict[table_name] = table_dict
                    # We can't easily get column types from SHOW TABLES, so we trust the metadata file for now.
                    # A "DESCRIBE <table_name>" query could be used for more detail if needed.
        else:
            print(f"Failed to list tables for metadata enhancement: {job.info}")

    except QuerySdkError as e:
        print(f"Error listing tables from EMR Presto: {e}")


    _sfdc_metadata = json.dumps(_final_dict, indent=2)
    _sfdc_metadata_dict = _final_dict

def _sql_validator(sql_code: str) -> Tuple[str, str]:
    """SQL Validator. Validates Presto SQL query using EMR Serverless client.
    It doesn't execute the query, but sends it for validation (e.g., by running EXPLAIN).

        Args:
        sql_code (str): Presto SQL code to validate.

    Returns:
        tuple(str,str):
            str: "SUCCESS" if SQL is valid, error text otherwise.
            str: original SQL code (as we don't modify it in-place).
    """
    print("Running Presto SQL validator.")
    # Presto doesn't have a 'dry_run' config like BigQuery.
    # A common way to validate syntax is to prepend EXPLAIN to the query.
    # This checks the syntax and analyzes the query plan without executing it.
    validation_query = f"EXPLAIN {sql_code}"

    # Add the engine type setting
    full_query = f'set tqs.query.engine.type = presto; {validation_query}'

    client = ServerlessClient(credentials=StaticCredentials(_access_key, _secret_key),
                              region=_region, endpoint='open.volcengineapi.com', service='emr_serverless')
    try:
        job = client.execute(task=SQLTask(name=f'validate_sql_{uuid.uuid4().hex}', query=full_query, conf={"tqs.query.engine.type": "presto"}), is_sync=True)
        if job.is_success():
            print("SQL syntax appears valid.")
            return "SUCCESS", sql_code
        else:
            err_text = f"Query failed validation. Status: {job.status}. Info: {job.info}"
            print(err_text)
            return err_text, sql_code

    except QuerySdkError as e:
        err_text = f"ERROR: {e}"
        print(err_text)
        return err_text, sql_code
    except Exception as ex:
        err_text = f"An unexpected error occurred during validation: {ex}"
        print(err_text)
        return err_text, sql_code


class SQLResult(BaseModel):
    sql_code: str
    sql_code_file_name: str
    error: str = ""


######## AGENT ########
async def data_engineer(request: str, tool_context: ToolContext) -> SQLResult:
    """
    This is your Senior Data Engineer.
    They have extensive experience in working with CRM data.
    They write clean and efficient SQL in its Presto dialect for querying EMR Serverless.
    When given a question or a set of steps,
    they can understand whether the problem can be solved with the data you have.
    The result is a Presto SQL Query.
    """
    _init_environment()
    # The prompt needs to be updated to specify Presto SQL dialect
    prompt = data_engineer_prompt.format(
        request=request,
        # These project/dataset IDs are no longer relevant in the same way
        data_project_id=_database,
        dataset=_database,
        sfdc_metadata=_sfdc_metadata
    )

    # This part is now replaced with the Volcengine LLM SDK
    messages = [
        {"role": "system", "content": data_engineer_instruction},
        {"role": "user", "content": prompt}
    ]

    try:
        completion = get_volcengine_llm_client().chat.completions.create(
            model=DATA_ENGINEER_AGENT_MODEL_ID,
            messages=messages,
            temperature=0.1,
            max_tokens=4096,
        )
        sql = completion.choices[0].message.content

        # The original code expected a SQLResult object. Since the new API returns
        # raw text, we create the object manually to maintain compatibility with
        # the rest of the function.
        sql_result = SQLResult(sql_code=sql, sql_code_file_name="")

    except Exception as e:
        raise RuntimeError(f"Volcengine Maas API call failed: {e}")

    print(f"SQL Query candidate: {sql}")

    MAX_FIX_ATTEMPTS = 3
    validating_query = sql
    is_good = False

    for i in range(MAX_FIX_ATTEMPTS):
        print(f"SQL Validation Attempt {i+1}/{MAX_FIX_ATTEMPTS}")
        validator_result, validating_query = _sql_validator(validating_query)
        if validator_result == "SUCCESS":
            is_good = True
            break
        print(f"ERROR: {validator_result}")

        # This correction loop will also need to use the Volcengine LLM
        correction_prompt = sql_correction_prompt.format(
            sql_query=validating_query,
            error_message=validator_result,
            sfdc_metadata=_sfdc_metadata,
            data_project_id=_database,
            dataset=_database,
        )
        correction_messages = [
            {"role": "system", "content": sql_correction_instruction},
            {"role": "user", "content": correction_prompt}
        ]
        correction_completion = get_volcengine_llm_client().chat.completions.create(
            model=SQL_VALIDATOR_MODEL_ID,
            messages=correction_messages,
            temperature=0.0,
            max_tokens=4096,
        )
        # The original code parsed a SQLResult object. The new API returns raw text.
        # We assume the model is prompted to return only the SQL code.
        validating_query = correction_completion.choices[0].message.content
        print(f"Corrected SQL Query candidate: {validating_query}")


    if not is_good:
        print("Failed to generate a valid SQL query after multiple attempts.")
        # Return the last failed attempt with the error
        sql_result.sql_code = validating_query
        sql_result.error = validator_result
        return sql_result

    # Save the validated SQL to a file
    file_name = f"presto_query_{uuid.uuid4().hex}.sql"
    # This should be saved to a shared location, possibly TOS, if the agent is distributed
    with open(file_name, "w", encoding="utf-8") as f:
        f.write(validating_query)

    sql_result.sql_code = validating_query
    sql_result.sql_code_file_name = file_name
    sql_result.error = ""

    return sql_result
