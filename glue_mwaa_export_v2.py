import json
import os
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.models import Variable
import boto3

## AWS Glue Settings

# AWS Region where connection and job will be executed
AWS_REGION= Variable.get("aws_region", default_var="us-east-1")

# Glue Execution Role
AWS_GLUE_ROLE_NAME = Variable.get("glue_role_name", "your-iam-role-name")

# Target S3 bucket
S3_BUCKET = Variable.get("export_s3_bucket", "your-s3-bucket-name")

# Script location
SCRIPT_LOCATION = f's3://{S3_BUCKET}/scripts/mwaa_metadb_export.py'

# Define data window
MAX_AGE_IN_DAYS = 30

# You can add other objects to export from the metadatabase,
# Define the tables and the date fields
OBJECTS_TO_EXPORT = [
    {"table": "dag_run", "date_field": "execution_date"},
    {"table": "task_fail", "date_field": "start_date"},
    {"table": "task_instance", "date_field": "start_date"},
    {"table": "serialized_dag", "date_field": "last_updated"},
]

# Do not edit below this line
ENV_NAME = os.getenv("AIRFLOW_ENV_NAME")
DAG_ID = os.path.basename(__file__).replace(".py", "")

# Task to create a Glue connection
@task()
def create_glue_connection():
    
    sql_alchemy_conn = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")    
    if sql_alchemy_conn is None:
        # Retry with the new environment variable name
        sql_alchemy_conn = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
        
    if sql_alchemy_conn is None:
        raise ValueError("SQL_ALCHEMY_CONN environment variable is not set.")
    
    conn_name = f'{ENV_NAME}_conn'
    glue_client = boto3.client('glue', region_name=AWS_REGION)

    # Check if Glue connection name exists
    try:
        glue_client.get_connection(Name=conn_name)
        print(f"Connection {conn_name} already exists.")
        return conn_name
    except glue_client.exceptions.EntityNotFoundException:
        pass

    # Parse JDBC Connection String
    s = sql_alchemy_conn.split('@')
    s2 = s[1].split('?')[0]
    jdbc_connection_url = f'jdbc:postgresql://{s2}'
    c = s[0].split('//')[1].split(':')
    username = c[0]
    password = c[1]

    # Export environment network configuration
    mwaa_client = boto3.client('mwaa', region_name=AWS_REGION)
    response = mwaa_client.get_environment(Name=ENV_NAME)
    mwaa_security_group_ids = response['Environment']['NetworkConfiguration']['SecurityGroupIds']
    mwaa_subnet_id = response['Environment']['NetworkConfiguration']['SubnetIds'][0]

    ec2_client = boto3.client('ec2', region_name=AWS_REGION)
    response = ec2_client.describe_subnets(SubnetIds=[mwaa_subnet_id], DryRun=False)
    mwaa_subnet_az = response['Subnets'][0]['AvailabilityZone']

    # Create new AWS Glue connection
    mwaa_metadb_conn = {
        'Name': conn_name,
        'Description': f'{ENV_NAME} MetaDB connection',
        'ConnectionType': 'JDBC',
        'MatchCriteria': ['string'],
        'ConnectionProperties': {
            'JDBC_ENFORCE_SSL': 'false',
            'JDBC_CONNECTION_URL': jdbc_connection_url,
            'PASSWORD': password,
            'USERNAME': username,
            'KAFKA_SSL_ENABLED': 'false'
        },
        'PhysicalConnectionRequirements': {
            'SubnetId': mwaa_subnet_id,
            'SecurityGroupIdList': mwaa_security_group_ids,
            'AvailabilityZone': mwaa_subnet_az
        }
    }

    try:
        glue_client.create_connection(ConnectionInput=mwaa_metadb_conn)
        print(f"Connection {conn_name} created successfully.")
    except Exception as e:
        print(f"Failed to create connection: {e}")
    
    return conn_name

# Define the DAG
@dag(dag_id=DAG_ID, schedule_interval=None, start_date=days_ago(1))
def glue_mwaa_export_dag():
    
    # Task 1: Create Glue Connection
    glue_connection_name = create_glue_connection()

    # Task 2: Run Glue Job and export table data in CSV.
    glue_job_name = f"{ENV_NAME}_export"
    database_name = Variable.get("database_name", default_var="airflow_metadb")

    glue_job_task = GlueJobOperator(
        task_id="run_glue_job",
        job_name=glue_job_name,
        script_location=SCRIPT_LOCATION,
        s3_bucket=S3_BUCKET,
        region_name=AWS_REGION,
        iam_role_name=AWS_GLUE_ROLE_NAME,
        script_args={
            '--S3_OUTPUT_PATH': f"s3://{S3_BUCKET}/exports/{ENV_NAME}",
            '--DATABASE_NAME': database_name,
            '--EXPORT_TABLES': json.dumps(OBJECTS_TO_EXPORT),
            '--GLUE_CONNECTION_NAME': glue_connection_name,
            '--MAX_AGE_IN_DAYS': str(MAX_AGE_IN_DAYS),
        },
        create_job_kwargs={
            'NumberOfWorkers': 1,
            'WorkerType': 'Standard',
            'GlueVersion': '4.0',
            'Connections': {
                'Connections': [glue_connection_name]
            }            
        },
        do_xcom_push=True,
    )

    glue_connection_name >> glue_job_task

# Instantiate the DAG
glue_mwaa_export_dag_d = glue_mwaa_export_dag()