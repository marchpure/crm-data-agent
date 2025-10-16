#!/bin/bash
###
 # @Author: haoxingjun
 # @Date: 2025-10-14 14:30:34
 # @Email: haoxingjun@bytedance.com
 # @LastEditors: haoxingjun
 # @LastEditTime: 2025-10-16 02:20:04
 # @Description: file information
 # @Company: ByteDance
### 
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

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd "${SCRIPT_DIR}/src" &> /dev/null || exit
uvicorn --app-dir web fast_api_runner:api_app --port 8000 & python3 web/main.py "agents/data_agent" "local" & wait
popd &> /dev/null || exit
