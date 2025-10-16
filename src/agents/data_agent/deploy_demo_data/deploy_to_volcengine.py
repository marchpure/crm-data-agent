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
from pathlib import Path
from dotenv import load_dotenv

# Construct the path to the .env file located in the src directory
script_dir = Path(__file__).parent
src_dir = script_dir.parent.parent.parent # Navigate up to the src directory
dotenv_path = src_dir / '.env'

# Load environment variables from the specified .env file
print(f"Loading environment variables from: {dotenv_path}")
load_dotenv(dotenv_path=dotenv_path)

import tos
from pathlib import Path
import sys
import time
import json
import re
import pyarrow.parquet as pq
import tos

from serverless.client import ServerlessClient
from serverless.auth import StaticCredentials
from serverless.task import SQLTask
from serverless.exceptions import QuerySdkError

from serverless.exceptions import QuerySdkError
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Optional, Dict, Any
from volcengine.auth.SignerV4 import SignerV4 
from volcengine.base.Request import Request 
from volcengine.Credentials import Credentials 

from volcengine.base.Service import Service
from volcengine.ServiceInfo import ServiceInfo
from las_rest.core.catalog_client import CatalogClient
from las_rest.core.entity.database_builder import DatabaseBuilder

def ensure_catalog_exists(las_client: CatalogClient, catalog_name: str, location: str):
    print(f"Checking if catalog '{catalog_name}' exists...")
    try:
        catalog_list = las_client.list_catalogs()
        if catalog_name in catalog_list:
            print(f"Catalog '{catalog_name}' already exists.")
            return True
        
        print(f"Catalog '{catalog_name}' not found. Creating it...")
        result = las_client.create_catalog(catalog_name=catalog_name, location=location, description="Created by crm-data-agent")
        if result:
            print(f"Successfully created catalog '{catalog_name}'.")
            return True
        else:
            print(f"Failed to create catalog '{catalog_name}'.")
            return False
    except Exception as e:
        print(f"Error ensuring catalog '{catalog_name}' exists: {e}")
        return False

def ensure_database_exists(las_client: CatalogClient, catalog_name: str, db_name: str, location: str):
    print(f"Checking if database '{db_name}' exists in catalog '{catalog_name}'...")
    try:
        # Attempt to get the database directly to see if it exists
        las_client.get_database(catalog_name, db_name)
        print(f"Database '{db_name}' already exists.")
        return True
    except Exception:
        # If the database is not found, create it
        print(f"Database '{db_name}' not found. Creating it...")
        try:
            database = DatabaseBuilder(name=db_name, location=location, catalog_name=catalog_name).with_description("Created by crm-data-agent").build()
            if las_client.create_database(database):
                print(f"Successfully created database '{db_name}'.")
                return True
            else:
                print(f"Failed to create database '{db_name}'.")
                return False
        except Exception as create_e:
            print(f"Error creating database '{db_name}': {create_e}")
            return False
    except Exception as e:
        print(f"Error ensuring database '{db_name}' exists: {e}")
        return False


# Define default values
DEFAULT_REGION = "cn-beijing"
DEFAULT_TOS_PREFIX = "crm-data-agent/parquet-data"
DEFAULT_CATALOG = "emr_catalog"
DEFAULT_DATABASE = "crm_data"


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

def main():
    # Load configuration from environment variables
    access_key = os.environ["VOLCENGINE_AK"]
    secret_key = os.environ["VOLCENGINE_SK"]
    region = os.environ.get("VOLCENGINE_REGION", DEFAULT_REGION)
    tos_bucket = os.environ["VE_TOS_BUCKET"]
    tos_prefix = os.environ.get("VE_TOS_PREFIX", DEFAULT_TOS_PREFIX)
    catalog = os.environ.get("EMR_CATALOG", DEFAULT_CATALOG)
    database = os.environ.get("EMR_DATABASE", DEFAULT_DATABASE)
    parquet_folder = os.environ.get("VE_PARQUET_FOLDER", str(Path(__file__).parent / "sample-data"))
    skip_upload = os.environ.get("VE_DEPLOY_SKIP_UPLOAD", "false").lower() == "true"
    skip_create_tables = os.environ.get("VE_DEPLOY_SKIP_CREATE_TABLES", "false").lower() == "true"
    skip_query = os.environ.get("VE_DEPLOY_SKIP_QUERY", "false").lower() == "true"

    # Create LAS client
    las_client = CatalogClient(access_key=access_key, secret_key=secret_key, region=region)

    # Ensure Catalog and Database exist
    catalog_location = f"tos://{tos_bucket}"
    if not ensure_catalog_exists(las_client, catalog, catalog_location):
        return
    # Dynamically create database location from bucket name
    db_location = f"tos://{tos_bucket}/{database}"
    if not ensure_database_exists(las_client, catalog, database, db_location):
        return

    # The rest of the main function remains the same
    uploaded_files = []
    if not skip_upload:
        uploaded_files = upload_to_tos(
            access_key,
            secret_key,
            region,
            tos_bucket,
            tos_prefix,
            parquet_folder
        )
    else:
        abs_parquet_folder = os.path.abspath(parquet_folder)
        for filename in os.listdir(abs_parquet_folder):
            if filename.endswith(".parquet"):
                tos_path = f"tos://{tos_bucket}/{tos_prefix}/{filename}"
                uploaded_files.append((filename, tos_path))
    
    if not uploaded_files:
        print("No files to process. Exiting.")
        return

    if not skip_create_tables:
        execute_sql_from_file(
            access_key,
            secret_key,
            region
        )
    
    if not skip_query:
        table_names = [filename[:-8] for filename, _ in uploaded_files]
        query_tables(
            access_key,
            secret_key,
            region,
            catalog,
            database,
            table_names
        )


if __name__ == "__main__":
    main()