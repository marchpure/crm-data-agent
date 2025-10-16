import os
import json
from pathlib import Path
from dotenv import load_dotenv
from serverless.client import ServerlessClient
from serverless.auth import StaticCredentials
from serverless.task import SQLTask
import sys

# Add src directory to Python path to allow absolute imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from agents.data_agent.tools.utils import get_volcengine_llm_client

def generate_description_with_llm(client, catalog, database, table_name, column_name, column_type):
    """Generates a column description using an LLM by sampling data."""
    print(f"    -> Generating description for {column_name} using LLM...")
    try:
        sample_data = []
        try:
            query = f'set tqs.query.engine.type = presto; SELECT "{column_name}" FROM "{catalog}"."{database}"."{table_name}" WHERE "{column_name}" IS NOT NULL LIMIT 5'
            job = client.execute(task=SQLTask(name=f'sample_{table_name}_{column_name}', query=query, conf={"tqs.query.engine.type": "presto"}), is_sync=True)
            if job.is_success():
                sample_data = [row[0] for row in job.get_result()]
            else:
                print(f"      [Warning] Failed to sample data, proceeding without it: {job.info}")
        except Exception as e:
            print(f"      [Warning] Error sampling data, proceeding without it: {e}")

        # 2. Construct the prompt
        prompt = (
            f"You are a data analyst creating documentation for a CRM database.\n"
            f"Given the following information about a database column:\n"
            f"- Table Name: `{table_name}`\n"
            f"- Column Name: `{column_name}`\n"
            f"- Data Type: `{column_type}`\n"
        )
        if sample_data:
            prompt += f"- Sample Values: `{sample_data}`\n\n"
        else:
            prompt += "\n"
        
        prompt += (
            f"Please generate a concise, one-sentence business description for this column. "
            f"For example: 'The estimated annual revenue of the account.'\n"
            f"Do not output anything else besides the single sentence description."
        )

        # 3. Call the LLM
        llm_client = get_volcengine_llm_client()
        model_id = os.environ["VE_LLM_MODEL_ID"]
        
        completion = llm_client.chat.completions.create(
            model=model_id,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=100,
        )
        description = completion.choices[0].message.content.strip()
        print(f"      LLM generated: '{description}'")
        return description

    except Exception as e:
        print(f"      Error generating description with LLM: {e}")
        return ""

def main():
    """Refreshes the sfdc_metadata.json file by scanning tables in EMR Presto."""
    # Load environment variables
    dotenv_path = Path(__file__).parent.parent.parent / '.env'
    load_dotenv(dotenv_path=dotenv_path)

    access_key = os.environ["VOLCENGINE_AK"]
    secret_key = os.environ["VOLCENGINE_SK"]
    region = os.environ["VOLCENGINE_REGION"]
    catalog = os.environ["EMR_CATALOG"]
    database = os.environ["EMR_DATABASE"]
    metadata_file_path = Path(__file__).parent / "sfdc_metadata.json"

    # Initialize EMR Serverless client
    client = ServerlessClient(
        credentials=StaticCredentials(access_key, secret_key),
        region=region,
        endpoint='open.volcengineapi.com',
        service='emr_serverless'
    )

    print(f"Connecting to EMR Presto in catalog '{catalog}', database '{database}'...")

    # 1. Get list of tables
    try:
        query = f'set tqs.query.engine.type = presto; SHOW TABLES FROM "{catalog}"."{database}"'
        job = client.execute(task=SQLTask(name='list_tables_for_metadata_refresh', query=query, conf={"tqs.query.engine.type": "presto"}), is_sync=True)
        if not job.is_success():
            print(f"Failed to list tables: {job.info}")
            return
        tables = [row[0] for row in job.get_result()]
        print(f"Found {len(tables)} tables: {tables}")
    except Exception as e:
        print(f"Error listing tables: {e}")
        return

    # 2. Load existing metadata and extra descriptions
    old_metadata = {}
    extra_descriptions = {}
    if metadata_file_path.exists():
        with open(metadata_file_path, 'r', encoding='utf-8') as f:
            old_metadata = json.load(f)
        print("Successfully loaded existing metadata file.")
    
    extra_desc_path = Path(__file__).parent.parent.parent / 'metadata' / 'sfdc_extra_descriptions.json'
    if extra_desc_path.exists():
        with open(extra_desc_path, 'r', encoding='utf-8') as f:
            extra_descriptions = json.load(f)
        print("Successfully loaded extra descriptions file.")

    new_metadata = {}
    # 3. For each table, get schema and merge with old metadata
    for table_name in tables:
        print(f"\nProcessing table: {table_name}")
        try:
            query = f'set tqs.query.engine.type = presto; DESCRIBE "{catalog}"."{database}"."{table_name}"'
            job = client.execute(task=SQLTask(name=f'describe_{table_name}', query=query, conf={"tqs.query.engine.type": "presto"}), is_sync=True)
            if not job.is_success():
                print(f"  Failed to describe table: {job.info}")
                continue
            
            columns_data = job.get_result()
            columns = []
            for col_row in columns_data:
                col_name = col_row[0]
                col_type = col_row[1]
                
                # Prioritize descriptions: extra_descriptions > old_metadata > generate_with_llm
                description = ""
                if table_name in extra_descriptions and col_name in extra_descriptions[table_name]:
                    description = extra_descriptions[table_name][col_name]
                    print(f"    -> Found description for '{col_name}' in extra descriptions.")
                elif table_name in old_metadata and "columns" in old_metadata[table_name]:
                    old_col = next((c for c in old_metadata[table_name]["columns"] if c["name"] == col_name), None)
                    if old_col and old_col.get("sfdc_description"):
                        description = old_col["sfdc_description"]
                        print(f"    -> Found description for '{col_name}' in old metadata.")
                
                if not description:
                    description = generate_description_with_llm(client, catalog, database, table_name, col_name, col_type)

                columns.append({
                    "name": col_name,
                    "type": col_type,
                    "sfdc_description": description
                })
            
            # Preserve table-level description
            table_description = ""
            if table_name in extra_descriptions and "_table_description" in extra_descriptions[table_name]:
                table_description = extra_descriptions[table_name]["_table_description"]
            elif table_name in old_metadata:
                table_description = old_metadata[table_name].get("sfdc_description", "")

            new_metadata[table_name] = {
                "sfdc_description": table_description,
                "columns": columns
            }
            print(f"  Successfully processed {len(columns)} columns for table {table_name}.")

        except Exception as e:
            print(f"  Error processing table {table_name}: {e}")

    # 4. Write the new metadata back to the file
    try:
        with open(metadata_file_path, 'w', encoding='utf-8') as f:
            json.dump(new_metadata, f, indent=4)
        print(f"\nSuccessfully refreshed and saved metadata to {metadata_file_path}")
    except Exception as e:
        print(f"\nError writing new metadata file: {e}")

if __name__ == "__main__":
    main()