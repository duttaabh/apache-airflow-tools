"""
DAG to create Glue connection to MWAA metadata database (Airflow 3.x compatible).

This DAG creates a Glue connection that can be used by other processes
(like Lambda functions or other DAGs) to access the MWAA metadata database.

Trigger with configuration:
{
  "aws_region": "us-east-1"
}
"""

import json
import os
from datetime import datetime
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
import boto3

ENV_NAME = os.getenv("AIRFLOW_ENV_NAME")
DAG_ID = "create_glue_connection"


@task()
def create_glue_connection_task(**context):
    """Create Glue connection to MWAA metadata database (Airflow 3.x compatible)"""
    
    # Get parameters from DAG run config
    dag_conf = context['dag_run'].conf or {}
    aws_region = dag_conf.get('aws_region', 'us-east-1')
    
    # Try to get SQL Alchemy connection string first (Airflow 2.x style)
    sql_alchemy_conn = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
    if sql_alchemy_conn is None:
        # Retry with the new environment variable name (Airflow 3.x)
        sql_alchemy_conn = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    
    # If SQL Alchemy connection is not available or blocked, try MWAA-specific variables (Airflow 3.x)
    if sql_alchemy_conn is None or sql_alchemy_conn.startswith("airflow-db-not-allowed"):
        print("SQL Alchemy connection not available, trying MWAA-specific environment variables...")
        
        # Get MWAA database credentials (Airflow 3.x)
        mwaa_db_credentials = os.getenv("DB_SECRETS")
        mwaa_db_host = os.getenv("POSTGRES_HOST")
        mwaa_db_port = os.getenv("POSTGRES_PORT", "5432")
        mwaa_db_name = os.getenv("POSTGRES_DB", "AirflowMetadata")
        
        if not all([mwaa_db_credentials, mwaa_db_host]):
            raise AirflowException("Neither SQL_ALCHEMY_CONN nor MWAA database environment variables are available.")
        
        # Parse MWAA credentials JSON
        try:
            credentials = json.loads(mwaa_db_credentials)
            username = credentials["username"]
            password = credentials["password"]
        except (json.JSONDecodeError, KeyError) as e:
            raise AirflowException(f"Failed to parse DB_SECRETS: {e}")
        
        # Construct JDBC URL from MWAA variables
        jdbc_connection_url = f"jdbc:postgresql://{mwaa_db_host}:{mwaa_db_port}/{mwaa_db_name}"
        
        print(f"Using MWAA database variables:")
        print(f"  Host: {mwaa_db_host}")
        print(f"  Port: {mwaa_db_port}")
        print(f"  Database: {mwaa_db_name}")
        print(f"  Username: {username}")
        print(f"  JDBC URL: {jdbc_connection_url}")
        
    else:
        # Parse traditional SQL Alchemy connection string (Airflow 2.x)
        print(f"Using SQL Alchemy Connection: {sql_alchemy_conn}")
        
        # Parse JDBC Connection String
        s = sql_alchemy_conn.split('@')
        s2 = s[1].split('?')[0]
        jdbc_connection_url = f'jdbc:postgresql://{s2}'
        c = s[0].split('//')[1].split(':')
        username = c[0]
        password = c[1]
        
        print(f"Parsed JDBC URL: {jdbc_connection_url}")
        print(f"Username: {username}")
    
    conn_name = f'{ENV_NAME}_metadata_conn'
    glue_client = boto3.client('glue', region_name=aws_region)

    # Check if connection exists
    try:
        glue_client.get_connection(Name=conn_name)
        print(f"✓ Connection {conn_name} already exists.")
        return {
            'connection_name': conn_name,
            'status': 'exists',
            'message': f'Connection {conn_name} already exists'
        }
    except glue_client.exceptions.EntityNotFoundException:
        pass

    # Get MWAA network configuration
    mwaa_client = boto3.client('mwaa', region_name=aws_region)
    response = mwaa_client.get_environment(Name=ENV_NAME)
    mwaa_security_group_ids = response['Environment']['NetworkConfiguration']['SecurityGroupIds']
    mwaa_subnet_id = response['Environment']['NetworkConfiguration']['SubnetIds'][0]

    ec2_client = boto3.client('ec2', region_name=aws_region)
    response = ec2_client.describe_subnets(SubnetIds=[mwaa_subnet_id])
    mwaa_subnet_az = response['Subnets'][0]['AvailabilityZone']

    # Create Glue connection
    connection_input = {
        'Name': conn_name,
        'Description': f'{ENV_NAME} metadata database connection for role creation',
        'ConnectionType': 'JDBC',
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
        glue_client.create_connection(ConnectionInput=connection_input)
        print(f"✓ Connection {conn_name} created successfully.")
        return {
            'connection_name': conn_name,
            'status': 'created',
            'message': f'Connection {conn_name} created successfully'
        }
    except Exception as e:
        raise AirflowException(f"Failed to create connection: {e}")


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule='@once',  # Run once automatically when DAG is uploaded
    catchup=False,
    tags=['glue', 'setup', 'admin'],
    params={
        "aws_region": "us-east-1"
    },
    description="Creates Glue connection to MWAA metadata database for external access",
    is_paused_upon_creation=False  # Enable DAG automatically
)
def create_glue_connection_dag():
    """DAG to create Glue connection for MWAA metadata database access"""
    
    result = create_glue_connection_task()
    
    return result


# Instantiate the DAG
create_glue_connection_dag_instance = create_glue_connection_dag()
