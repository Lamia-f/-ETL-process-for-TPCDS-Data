import os
import boto3
import requests
import snowflake.connector as sf


def download_file(url, destination_folder, file_name):
    response = requests.get(url)
    response.raise_for_status()
    file_path = os.path.join(destination_folder, file_name)
    with open(file_path, 'wb') as file:
        file.write(response.content)
    return file_path


def lambda_handler(event, context):
    url = 'https://de-materials-tpcds.s3.ca-central-1.amazonaws.com/inventory.csv'
    destination_folder = '/tmp'
    file_name = 'inventory.csv'
    local_file_path = download_file(url, destination_folder, file_name)
    
    # Snowflake connection parameters
    account = 'YOEJPQE-QXB52948'
    warehouse = 'COMPUTE_WH'
    database = 'tpcds'
    schema = 'raw'
    table = 'inventory'
    user = 'wcd_midterm_load_user'
    password = 'wcdmidtermloaduser12345'
    role = 'accountadmin'
    stage_name = 'inventory_Stage'
    file_format_name = 'comma_csv'

    # Establish Snowflake connection
    conn = sf.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role
    )
    cursor = conn.cursor()
    
    # Use warehouse and schema
    use_warehouse_query = f"USE WAREHOUSE {warehouse};"
    cursor.execute(use_warehouse_query)
    use_schema_query = f"USE SCHEMA {schema};"
    cursor.execute(use_schema_query)

    # Create CSV format
    create_csv_format_query = f"CREATE OR REPLACE FILE FORMAT {file_format_name} TYPE = 'CSV' FIELD_DELIMITER = ',';"
    cursor.execute(create_csv_format_query)

    # Create stage
    create_stage_query = f"CREATE OR REPLACE STAGE {stage_name} FILE_FORMAT = {file_format_name};"
    cursor.execute(create_stage_query)

    # Copy the file from local to the stage
    copy_into_stage_query = f"PUT 'file://{local_file_path}' @{stage_name};"
    cursor.execute(copy_into_stage_query)

    # Truncate table
    truncate_table_query = f"TRUNCATE TABLE {schema}.{table};"
    cursor.execute(truncate_table_query)

    # Load the data from the stage into the table
    copy_into_query = f"COPY INTO {schema}.{table} FROM @{stage_name}/{file_name} FILE_FORMAT = {file_format_name} ON_ERROR = 'continue';"
    cursor.execute(copy_into_query)

    print("File uploaded to Snowflake successfully.")

    return {
        'statusCode': 200,
        'body': 'File downloaded and uploaded to Snowflake successfully.'
    }