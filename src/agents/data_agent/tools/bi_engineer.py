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
"""BI Engineer Agent"""

from datetime import date, datetime
from functools import cache
import io
import json
import os

from pydantic import BaseModel

from google.adk.tools import ToolContext
from google.genai.types import (
    GenerateContentConfig,
    Part,
    SafetySetting,
    ThinkingConfig
)
from serverless.client import ServerlessClient
from serverless.auth import StaticCredentials
from serverless.task import SQLTask
from serverless.exceptions import QuerySdkError

import altair as alt
from altair.vegalite.schema import core as alt_core
import pandas as pd

from .utils import get_volcengine_llm_client
from prompts.bi_engineer import prompt as bi_engineer_prompt
from tools.chart_evaluator import evaluate_chart


MAX_RESULT_ROWS_DISPLAY = 50
MODEL_ID = os.environ.get("VE_LLM_MODEL_ID", "doubao-pro-32k")
BI_ENGINEER_AGENT_MODEL_ID = MODEL_ID
BI_ENGINEER_FIX_AGENT_MODEL_ID = MODEL_ID


@cache
def _init_environment():
    global _access_key, _secret_key, _region, _catalog, _database
    _access_key = os.environ["VOLCENGINE_AK"]
    _secret_key = os.environ["VOLCENGINE_SK"]
    _region = os.environ["VOLCENGINE_REGION"]
    _catalog = os.environ["EMR_CATALOG"]
    _database = os.environ["EMR_DATABASE"]

class VegaResult(BaseModel):
    vega_lite_json: str


def _enhance_parameters(vega_chart: dict, df: pd.DataFrame) -> dict:
    """
    Makes sure all chart parameters with "select" equal to "point"
    have the same option values as respective dimensions.

    Args:
        vega_chart_json (str): _description_
        df (pd.DataFrame): _description_

    Returns:
        str: _description_
    """
    if "params" not in vega_chart:
        return vega_chart
    if "params" not in vega_chart or "'transform':" not in str(vega_chart):
        print("Cannot enhance parameters because one or "
              "more of these are missing: "
              "[params, transform]")
        return vega_chart
    print("Enhancing parameters...")
    params_list = vega_chart["params"]
    params = { p["name"]: p for p in params_list }
    for p in params:
        if not p.endswith("__selection"):
            continue
        print(f"Parameter {p}")
        param_dict = params[p]
        column_name = p.split("__selection")[0]
        if column_name not in df.columns:
            print(f"Column {column_name} not found in dataframe.")
            continue
        field_values = df[column_name].unique().tolist()
        if None not in field_values:
            field_values.insert(0, None)
            none_index = 0
        else:
            none_index = field_values.index(None)
        param_dict["value"] = None
        param_dict["bind"] = {"input": "select"}
        param_dict["bind"]["options"] = field_values
        field_labels = field_values.copy()
        field_labels[none_index] = "[All]"
        param_dict["bind"]["labels"] = field_labels
        param_dict["bind"]["name"] = column_name
        print(f"Yay! We can filter by {column_name} now!")
    return vega_chart


def _create_chat(model: str, history: list, max_thinking: bool = False):
    messages = history + [{"role": "user", "content": ""}]
    completion = get_volcengine_llm_client().chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.1,
        max_tokens=4096,
    )
    return completion



def _fix_df_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts all columns of type date, datetime, or datetimetz in a
    Pandas DataFrame to ISO 8601 string format.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: A DataFrame with date/datetime columns converted to
                      ISO formatted strings.
    """
    df = df.copy()  # Work on a copy to avoid side effects
    # --- Process native pandas datetime types ---
    datetime_cols = df.select_dtypes(
        include=["datetime", "datetimetz", "dbdate"]
    ).columns
    for col in datetime_cols:
        # 1. Convert each value to an ISO string
        iso_values = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
        # 2. Explicitly cast the column to the modern 'string' dtype
        df[col] = iso_values.astype("string")

    # --- Process object columns that might contain date/datetime objects ---
    object_cols = df.select_dtypes(include=['object']).columns
    for col in object_cols:
        # Heuristic to find columns that contain date/datetime objects
        first_valid_index = df[col].first_valid_index()
        if first_valid_index is not None and isinstance(df[col].loc[first_valid_index], (date, datetime)):
            # 1. Convert each value to an ISO string
            iso_values = df[col].apply(
                lambda x: x.isoformat()
                if isinstance(x, (date, datetime))
                else x
            )
            # 2. Explicitly cast the column to the modern 'string' dtype
            df[col] = iso_values.astype("string")
    return df


def _json_date_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def _safe_json(json_str: str) -> str:
    json_str = "{" + json_str.strip().split("{", 1)[-1]
    json_str = json_str.rsplit("}", 1)[0] + "}"
    json_dict = json.loads(json_str)
    return json.dumps(json_dict, default=_json_date_serial)

async def bi_engineer_tool(original_business_question: str,
                     question_that_sql_result_can_answer: str,
                     sql_file_name: str,
                     notes: str,
                     tool_context: ToolContext) -> str:
    """Senior BI Engineer. Executes SQL code.

    Args:
        original_business_question (str): Original business question.
        question_that_sql_result_can_answer (str):
            Specific question or sub-question that SQL result can answer.
        sql_file_name (str): File name of BigQuery SQL code execute.
        notes (str): Important notes about the chart. Not empty only if the user stated something directly related to the chart.

    Returns:
        str: Chart image id and the result of executing the SQL code
             in CSV format (first 50 rows).
    """
    _init_environment()
    sql_code_part = await tool_context.load_artifact(sql_file_name)
    sql_code = sql_code_part.inline_data.data.decode("utf-8") # type: ignore
    client = ServerlessClient(credentials=StaticCredentials(_access_key, _secret_key),
                              region=_region, endpoint='open.volcengineapi.com', service='emr_serverless')
    try:
        full_query = f'set tqs.query.engine.type = presto; {sql_code}'
        job = client.execute(task=SQLTask(name=f'execute_sql_{uuid.uuid4().hex}', query=full_query, conf={"tqs.query.engine.type": "presto"}), is_sync=True)
        if job.is_success():
            result = job.get_result()
            # Assuming the result is a list of lists, convert to DataFrame
            if result:
                df = pd.DataFrame(result[1:], columns=result[0])
            else:
                df = pd.DataFrame()
        else:
            err_text = f"Query failed execution. Status: {job.status}. Info: {job.info}"
            return f"EMR PRESTO ERROR: {err_text}"

        df = _fix_df_dates(df)
    except QuerySdkError as ex:
        err_text = f"ERROR: {ex}"
        return f"EMR PRESTO ERROR: {err_text}"

    if notes:
        notes_text = f"\n\n**Important notes about the chart:** \n{notes}\n\n"
    else:
        notes_text = ""

    vega_lite_spec = json.dumps(
        alt_core.load_schema(),
        indent=1,
        sort_keys=False
    )
    chart_prompt = bi_engineer_prompt.format(
        original_business_question=original_business_question,
        question_that_sql_result_can_answer=question_that_sql_result_can_answer,
        sql_code=sql_code,
        notes_text=notes_text,
        columns_string=df.dtypes.to_string(),
        dataframe_preview_len=min(10,len(df)),
        dataframe_len=len(df),
        dataframe_head=df.head(10).to_string(),
        vega_lite_spec=vega_lite_spec,
        vega_lite_schema_version=alt.SCHEMA_VERSION.split(".")[0]
    )

    vega_chart_json = ""
    messages = []
    messages = [
        {"role": "user", "content": chart_prompt}
    ]
    completion = get_volcengine_llm_client().chat.completions.create(
        model=BI_ENGINEER_AGENT_MODEL_ID,
        messages=messages,
        temperature=0.1,
        max_tokens=8192,
    )
    chart_json = completion.choices[0].message.content
    messages.append({"role": "assistant", "content": chart_json})

    for _ in range(5): # 5 tries to make a good chart
        for _ in range(10):
            try:
                vega_dict = json.loads(_safe_json(chart_json)) # type: ignore
                vega_dict["data"] = {"values": []}
                vega_dict.pop("datasets", None)
                vega_chart = alt.Chart.from_dict(vega_dict)
                with io.BytesIO() as tmp:
                    vega_chart.save(tmp, "png")
                vega_dict = _enhance_parameters(vega_dict, df)
                vega_chart_json = json.dumps(vega_dict, indent=1)
                vega_chart = alt.Chart.from_dict(vega_dict)
                vega_chart.data = df
                with io.BytesIO() as file:
                    vega_chart.save(file, "png")
                error_reason = ""
                break
            except Exception as ex:
                message = f"""
                {chart_json}

                {df.dtypes.to_string()}

You made a mistake!
Fix the issues. Redesign the chart if it promises a better result.

ERROR {type(ex).__name__}: {str(ex)}
""".strip()
                error_reason = message
                print(message)
                messages.append({"role": "user", "content": message})
                completion = get_volcengine_llm_client().chat.completions.create(
                    model=BI_ENGINEER_FIX_AGENT_MODEL_ID,
                    messages=messages,
                    temperature=0.1,
                    max_tokens=8192,
                )
                chart_json = completion.choices[0].message.content
                messages.append({"role": "assistant", "content": chart_json})

        if not error_reason:
            with io.BytesIO() as file:
                vega_chart.data = df
                vega_chart.save(file, "png")
                file.seek(0)
                png_data = file.getvalue()
                evaluate_chart_result = evaluate_chart(
                                            png_data,
                                            vega_chart_json,
                                            question_that_sql_result_can_answer,
                                            len(df),
                                            tool_context)
            if not evaluate_chart_result or evaluate_chart_result.is_good:
                break
            error_reason = evaluate_chart_result.reason

        if not error_reason:
            break

        print(f"Feedback:\n{error_reason}.\n\nWorking on another version...")
        history = messages
        feedback_prompt = f"""
            Fix the chart based on the feedback.
            Only output Vega-Lite json.

            ***Feedback on the chart below**
            {error_reason}


            ***CHART**

            ``json
            {vega_chart_json}
            ````
            """
        messages.append({"role": "user", "content": feedback_prompt})
        completion = get_volcengine_llm_client().chat.completions.create(
            model=BI_ENGINEER_AGENT_MODEL_ID,
            messages=messages,
            temperature=0.1,
            max_tokens=8192,
        )
        chart_json = completion.choices[0].message.content
        messages.append({"role": "assistant", "content": chart_json})

    print(f"Done working on a chart.")
    if error_reason:
        print(f"Chart is still not good: {error_reason}")
    else:
        print("And the chart seem good to me.")
    data_file_name = f"{tool_context.invocation_id}.parquet"
    parquet_bytes = df.to_parquet()
    await tool_context.save_artifact(filename=data_file_name,
                               artifact=Part.from_bytes(
                                   data=parquet_bytes,
                                   mime_type="application/parquet"))
    file_name = f"{tool_context.invocation_id}.vg"
    await tool_context.save_artifact(filename=file_name,
                               artifact=Part.from_bytes(
                                    mime_type="application/json",
                                    data=vega_chart_json.encode("utf-8")))
    with io.BytesIO() as file:
        vega_chart.save(file, "png", ppi=72)
        file.seek(0)
        data = file.getvalue()
        new_image_name = f"{tool_context.invocation_id}.png"
        await tool_context.save_artifact(filename=new_image_name,
                                   artifact=Part.from_bytes(
                                        mime_type="image/png",
                                        data=data))
        tool_context.state["chart_image_name"] = new_image_name

    csv = df.head(MAX_RESULT_ROWS_DISPLAY).to_csv(index=False)
    if len(df) > MAX_RESULT_ROWS_DISPLAY:
        csv_message = f"**FIRST {MAX_RESULT_ROWS_DISPLAY} OF {len(df)} ROWS OF DATA**:"
    else:
        csv_message = "**DATA**:"

    return f"chart_image_id: `{new_image_name}`\n\n{csv_message}\n\n```csv\n{csv}\n```\n"
