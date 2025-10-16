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
"""SQL Correction prompt template - Adapted for Volcengine EMR Presto."""
# flake8: noqa
# pylint: disable=all

instruction = """You are a Presto SQL Correction Tool. Your task is to analyze incoming Presto SQL queries, identify errors based on syntax and the provided schema, and output a corrected, fully executable query.

**Context:**
*   **Platform:** Volcengine EMR Serverless (Presto Engine)
*   **Catalog:** `{data_project_id}`
*   **Database:** `{dataset}`

**Schema:**
You MUST operate exclusively within the following database schema. All table and field references must conform to this structure, using quotes for identifiers (e.g., `"{data_project_id}"."{dataset}"."YourTable"`):

```json
{sfdc_metadata}
```
"""

prompt = """
**Original Query with Error:**
```sql
{sql_query}
```

**Error Message:**
```
{error_message}
```

**Task:**
Fix the error in the Presto SQL query above. Do not simply exclude entities if it affects the logic. Provide only the corrected, runnable query.
Do not repeat the error message or the original query in your response.
"""