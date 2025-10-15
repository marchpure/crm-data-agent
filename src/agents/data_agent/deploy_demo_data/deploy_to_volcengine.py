#!/usr/bin/env python
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

import argparse
import os
import tos
from pathlib import Path
import sys
import time
import json
import re
import pyarrow.parquet as pq
import tos

# 导入test_presto_requests模块
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from test_presto_requests import create_presto_connection, execute_presto_query
from serverless.client import ServerlessClient
from serverless.auth import StaticCredentials
from serverless.task import SQLTask
from serverless.exceptions import QuerySdkError

# 配置常量
DEFAULT_REGION = "cn-beijing"
DEFAULT_TOS_BUCKET = "wangmeng-test"
DEFAULT_TOS_PREFIX = "sample-data"
DEFAULT_EMR_CLUSTER_ID = ""
DEFAULT_CATALOG = "hive_catalog"
DEFAULT_DATABASE = "test_db"

# 火山引擎API基础URL
def upload_to_tos(access_key, secret_key, region, bucket, prefix, parquet_folder):
    endpoint = f'tos-{region}.volces.com'
    client = tos.TosClientV2(access_key, secret_key, endpoint, region)
    uploaded_files = []
    parquet_folder = os.path.abspath(parquet_folder)

    if not os.path.isdir(parquet_folder):
        print(f"Error: Directory not found at {parquet_folder}")
        return uploaded_files

    print(f"Starting upload from {parquet_folder} to tos://{bucket}/{prefix}/")

    for filename in os.listdir(parquet_folder):
        if filename.endswith(".parquet"):
            # 提取文件名（不含.parquet后缀）作为子目录名
            table_name = filename[:-8]  # 移除.parquet后缀
            
            # 构建新的TOS路径，每个文件放在单独的子目录中
            local_path = os.path.join(parquet_folder, filename)
            subdirectory = f"{prefix}/{table_name}"
            tos_key = f"{subdirectory}/{filename}"

            print(f"Uploading {filename} to {tos_key}...")
            try:
                # 上传文件到子目录
                client.put_object_from_file(bucket, tos_key, local_path)
                tos_path = f"tos://{bucket}/{subdirectory}"  # 返回目录路径，而不是文件路径
                uploaded_files.append((filename, tos_path))
                print(f"Successfully uploaded {filename} to {tos_path}")
            except Exception as e:
                print(f"Failed to upload {filename}: {e}")

    return uploaded_files

def execute_sql_from_file(access_key, secret_key, region):
    """
    从指定的SQL文件中读取并执行SQL语句
    """
    client = ServerlessClient(credentials=StaticCredentials(access_key, secret_key), 
                              region=region, endpoint='open.volcengineapi.com', service='emr_serverless',
                              connection_timeout=30, 
                              socket_timeout=30)

    # 固定从all_sql_operations.sql文件读取SQL语句
    sql_file_path = "all_sql_operations.sql"
    print(f"从固定文件读取SQL语句: {sql_file_path}")
    
    # 检查文件是否存在
    if not os.path.exists(sql_file_path):
        print(f"错误: 找不到SQL文件 {sql_file_path}")
        print(f"请先确保 {sql_file_path} 文件存在于当前目录")
        return
    
    try:
        # 读取SQL文件内容
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
            
        # 移除文件头注释（以--开头的行）
        lines = sql_content.split('\n')
        filtered_lines = []
        in_header = True
        
        for line in lines:
            stripped_line = line.strip()
            # 跳过空行
            if not stripped_line:
                continue
            # 跳过文件头注释，但保留SQL语句中的注释
            if in_header and stripped_line.startswith('--'):
                print(f"跳过文件头注释: {stripped_line}")
                continue
            # 如果遇到非注释行，说明文件头结束
            if stripped_line and not stripped_line.startswith('--'):
                in_header = False
            
            filtered_lines.append(line)
        
        # 重新组合SQL内容
        cleaned_sql = '\n'.join(filtered_lines)
        
        # 分割SQL语句（按分号+换行符）
        sql_statements = [stmt.strip() for stmt in re.split(r';\s*\n', cleaned_sql) if stmt.strip()]
        
        print(f"共找到 {len(sql_statements)} 条SQL语句")
        
        # 逐个执行SQL语句
        success_count = 0
        fail_count = 0
        
        for i, sql_statement in enumerate(sql_statements):
            statement_name = f"sql_statement_{i+1}"
            
            # 尝试从SQL语句中提取表名（如果可能）
            table_name = None
            # 检查是否是CREATE TABLE语句
            if re.match(r'^\s*CREATE\s+(EXTERNAL\s+)?TABLE', sql_statement, re.IGNORECASE):
                match = re.search(r'CREATE\s+(EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)', sql_statement, re.IGNORECASE)
                if match:
                    table_name = match.group(2)
                    # 如果表名包含数据库前缀，只保留表名部分
                    if '.' in table_name:
                        table_name = table_name.split('.')[-1]
            # 检查是否是DROP TABLE语句
            elif re.match(r'^\s*DROP\s+TABLE', sql_statement, re.IGNORECASE):
                match = re.search(r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s;]+)', sql_statement, re.IGNORECASE)
                if match:
                    table_name = match.group(1)
                    if '.' in table_name:
                        table_name = table_name.split('.')[-1]
            
            # 如果找到表名，使用表名作为任务名称的一部分
            if table_name:
                statement_name = f"{table_name}_{statement_name}"
            
            print(f"\n执行第 {i+1}/{len(sql_statements)} 条SQL语句: {statement_name}")
            print(f"执行的SQL语句:\n{sql_statement}")
            
            try:
                # 单独执行每条SQL语句
                job = client.execute(task=SQLTask(name=statement_name, query=sql_statement + ';', conf={}), is_sync=True)
                
                if job.is_success():
                    print(f"成功执行SQL语句: {statement_name}")
                    success_count += 1
                else:
                    print(f"执行SQL语句失败: {statement_name}. Job Status: {job.status}")
                    print(f"Job Info: {job.info}")
                    fail_count += 1
                    
                    # 尝试获取失败日志
                    job_id_match = re.search(r'The job\[(\d+)\]', str(job.info))
                    if job_id_match:
                        job_id = job_id_match.group(1)
                        try:
                            submission_log = client.get_submission_log(job_id)
                            driver_log = client.get_driver_log(job_id)
                            print("\n--- Submission Log ---")
                            print(submission_log[:1000] + ('...' if len(submission_log) > 1000 else ''))
                            print("\n--- Driver Log ---")
                            print(driver_log[:2000] + ('...' if len(driver_log) > 2000 else ''))
                        except Exception as log_e:
                            print(f"获取日志失败: {log_e}")
            except Exception as e:
                print(f"执行SQL语句时发生异常: {statement_name}: {e}")
                fail_count += 1
                import traceback
                traceback.print_exc()
            
        # 输出执行结果摘要
        print(f"\nSQL执行摘要: 共 {len(sql_statements)} 条语句, 成功 {success_count} 条, 失败 {fail_count} 条")
        
        if fail_count > 0:
            print("警告: 部分SQL语句执行失败，请查看上面的详细信息")

    except Exception as e:
        print(f"执行SQL文件时发生错误: {e}")
        import traceback
        traceback.print_exc()

import requests
import base64
import json
import datetime
import os
import re

def query_tables(access_key, secret_key, region, catalog, database, table_names):
#     """
#     使用EMR Serverless Presto查询表数据
#     """
     client = ServerlessClient(credentials=StaticCredentials(access_key, secret_key), 
                               region=region, endpoint='open.volcengineapi.com', service='emr_serverless',
                               connection_timeout=30, 
                               socket_timeout=30)
     for table_name in table_names:
         query = f'set  tqs.query.engine.type = presto;SELECT * FROM "{catalog}"."{database}"."{table_name}" LIMIT 10'
         print(f"\nExecuting query for table: {table_name}")
         print(f"Executing SQL: {query}")
         

         try:
             job = client.execute(task
                  =SQLTask(name=f'query_table_{table_name}', query=query, conf={"tqs.query.engine.type": "presto"}), is_sync=True)
             if job.is_success():
                 result = job.get_result()
                 print(f"Query successful for {table_name}. Records:")
                 for record in result:
                     print(', '.join([str(col) for col in record]))
             else:
                 print(f"Failed to query table {table_name}. Job Status: {job.status}")
                 print(f"Job Info: {job.info}")

         except Exception as e:
             print(f"An error occurred while querying table {table_name}: {e}")



def main():
    parser = argparse.ArgumentParser(description="将Parquet文件上传到火山引擎TOS并使用EMR Serverless进行建表和查询")
    
    parser.add_argument("--access-key", help="火山引擎访问密钥", type=str, required=True)
    parser.add_argument("--secret-key", help="火山引擎密钥", type=str, required=True)
    parser.add_argument("--region", help="火山引擎区域", type=str, default=DEFAULT_REGION)
    parser.add_argument("--tos-bucket", help="TOS存储桶名称", type=str, default=DEFAULT_TOS_BUCKET)
    parser.add_argument("--tos-prefix", help="TOS文件路径前缀", type=str, default=DEFAULT_TOS_PREFIX)
    parser.add_argument("--catalog", help="LAS Catalog名称", type=str, default=DEFAULT_CATALOG)
    parser.add_argument("--database", help="LAS数据库名称", type=str, default=DEFAULT_DATABASE)
    parser.add_argument("--parquet-folder", help="本地Parquet文件目录", type=str, default=str(Path(__file__).parent / "sample-data"))
    parser.add_argument("--skip-upload", help="跳过上传文件到TOS", action="store_true")
    parser.add_argument("--skip-create-tables", help="跳过创建LAS表", action="store_true")
    parser.add_argument("--skip-query", help="跳过Presto查询", action="store_true")
    
    options = parser.parse_args(sys.argv[1:])
    
    uploaded_files = []
    if not options.skip_upload:
        uploaded_files = upload_to_tos(
            options.access_key,
            options.secret_key,
            options.region,
            options.tos_bucket,
            options.tos_prefix,
            options.parquet_folder
        )
    else:
        parquet_folder = os.path.abspath(options.parquet_folder)
        for filename in os.listdir(parquet_folder):
            if filename.endswith(".parquet"):
                tos_path = f"tos://{options.tos_bucket}/{options.tos_prefix}/{filename}"
                uploaded_files.append((filename, tos_path))
    
    if not uploaded_files:
        print("No files to process. Exiting.")
        return

    if not options.skip_create_tables:
        # 直接从SQL文件执行建表语句
        execute_sql_from_file(
            options.access_key,
            options.secret_key,
            options.region
        )
    
    if not options.skip_query:
        table_names = [filename[:-8] for filename, _ in uploaded_files]
        query_tables(
                options.access_key,
                options.secret_key,
                options.region,
                options.catalog,
                options.database,
                table_names
            )


if __name__ == "__main__":
    main()