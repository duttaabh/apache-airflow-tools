"""
DAG to create and execute a Glue job for MWAA custom role creation.

This DAG:
1. Creates a Glue connection to MWAA metadata database
2. Uploads the notebook script to S3
3. Creates a Glue job
4. Executes the Glue job to create the custom MWAA role

Trigger with configuration:
{
  "aws_region": "us-east-1",
  "stack_name": "{{VPC_STACK_NAME}}",
  "source_role": "User",
  "target_role": "MWAARestrictedTest",
  "specific_dags": ["hello_world_advanced", "hello_world_simple"]
}
"""

import json
import os
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.exceptions import AirflowException
import boto3

ENV_NAME = os.getenv("AIRFLOW_ENV_NAME")
DAG_ID = "create_role_glue_job"

# ============================================================================
# Task 1: Create Glue Connection
# ============================================================================
@task()
def create_glue_connection(**context):
    """Create Glue connection to MWAA metadata database"""
    
    # Get parameters from DAG run config
    dag_conf = context['dag_run'].conf or {}
    aws_region = dag_conf.get('aws_region', 'us-east-1')
    
    sql_alchemy_conn = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    if sql_alchemy_conn is None:
        sql_alchemy_conn = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
        
    if sql_alchemy_conn is None:
        raise AirflowException("SQL_ALCHEMY_CONN environment variable is not set.")
    
    conn_name = f'{ENV_NAME}_metadata_conn'
    glue_client = boto3.client('glue', region_name=aws_region)

    # Check if connection exists
    try:
        glue_client.get_connection(Name=conn_name)
        print(f"✓ Connection {conn_name} already exists.")
        return conn_name
    except glue_client.exceptions.EntityNotFoundException:
        pass

    # Parse JDBC connection string
    s = sql_alchemy_conn.split('@')
    s2 = s[1].split('?')[0]
    jdbc_connection_url = f'jdbc:postgresql://{s2}'
    c = s[0].split('//')[1].split(':')
    username = c[0]
    password = c[1]

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
    except Exception as e:
        raise AirflowException(f"Failed to create connection: {e}")
    
    return conn_name

@task()
def get_glue_role_arn(**context):
    """Get the Glue role ARN from CloudFormation stack outputs"""
    
    # Get parameters from DAG run config
    dag_conf = context['dag_run'].conf or {}
    aws_region = dag_conf.get('aws_region', 'us-east-1')
    stack_name = dag_conf.get('stack_name', '{{VPC_STACK_NAME}}')  # Replaced by deploy script
    
    cf_client = boto3.client('cloudformation', region_name=aws_region)
    
    try:
        response = cf_client.describe_stacks(StackName=stack_name)
        outputs = response['Stacks'][0]['Outputs']
        
        for output in outputs:
            if output['OutputKey'] == 'GlueRoleCreatorRoleArn':
                glue_role_arn = output['OutputValue']
                print(f"✓ Found Glue role ARN: {glue_role_arn}")
                return glue_role_arn
        
        raise AirflowException(f"GlueRoleCreatorRoleArn output not found in stack {stack_name}")
        
    except Exception as e:
        # Fallback to constructed ARN if stack lookup fails
        print(f"Warning: Could not get role from stack outputs: {e}")
        fallback_arn = f"arn:aws:iam::343218218212:role/{stack_name}-GlueRoleCreatorRole"
        print(f"Using fallback ARN: {fallback_arn}")
        return fallback_arn

@task()
def get_s3_bucket_name(**context):
    """Get the S3 bucket name from CloudFormation stack outputs"""
    
    # Get parameters from DAG run config
    dag_conf = context['dag_run'].conf or {}
    aws_region = dag_conf.get('aws_region', 'us-east-1')
    stack_name = dag_conf.get('stack_name', '{{VPC_STACK_NAME}}')  # Replaced by deploy script
    
    cf_client = boto3.client('cloudformation', region_name=aws_region)
    
    try:
        response = cf_client.describe_stacks(StackName=stack_name)
        outputs = response['Stacks'][0]['Outputs']
        
        for output in outputs:
            if output['OutputKey'] == 'MwaaS3BucketName':
                bucket_name = output['OutputValue']
                print(f"✓ Found S3 bucket: {bucket_name}")
                return bucket_name
        
        raise AirflowException(f"MwaaS3BucketName output not found in stack {stack_name}")
        
    except Exception as e:
        # Fallback to constructed bucket name if stack lookup fails
        print(f"Warning: Could not get bucket from stack outputs: {e}")
        fallback_bucket = f"{stack_name}-mwaa-343218218212"
        print(f"Using fallback bucket: {fallback_bucket}")
        return fallback_bucket

# ============================================================================
# Task 2: Upload Glue Script to S3
# ============================================================================
@task()
def upload_glue_script(**context):
    """Upload the Glue job script to S3"""
    
    # Get parameters from DAG run config
    dag_conf = context['dag_run'].conf or {}
    aws_region = dag_conf.get('aws_region', 'us-east-1')
    
    # Get S3 bucket from previous task
    s3_bucket = context['ti'].xcom_pull(task_ids='get_s3_bucket_name')
    
    # Glue job script (converted from notebook)
    glue_script = '''
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import boto3
import json

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'CONNECTION_NAME',
    'SOURCE_ROLE',
    'TARGET_ROLE',
    'SPECIFIC_DAGS'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

CONNECTION_NAME = args['CONNECTION_NAME']
SOURCE_ROLE = args['SOURCE_ROLE']
TARGET_ROLE = args['TARGET_ROLE']
SPECIFIC_DAGS = json.loads(args['SPECIFIC_DAGS'])

print(f"Creating role: {TARGET_ROLE} from {SOURCE_ROLE}")
print(f"Specific DAGs: {SPECIFIC_DAGS}")

# Load RBAC tables
tables = ['ab_role', 'ab_permission', 'ab_view_menu', 'ab_permission_view', 'ab_permission_view_role']
for table_name in tables:
    df = glueContext.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": f"public.{table_name}",
            "connectionName": CONNECTION_NAME,
        },
        transformation_ctx=table_name
    )
    df.toDF().createOrReplaceTempView(table_name)

# Query permissions - exclude wildcard DAG permissions
non_dag_query = f"""
SELECT pv.id as permission_view_id
FROM ab_role r
JOIN ab_permission_view_role pvr ON r.id = pvr.role_id
JOIN ab_permission_view pv ON pvr.permission_view_id = pv.id
JOIN ab_view_menu vm ON pv.view_menu_id = vm.id
JOIN ab_permission p ON pv.permission_id = p.id
WHERE r.name = '{SOURCE_ROLE}' 
  AND vm.name NOT LIKE 'DAG:%'
  AND vm.name NOT LIKE 'DAGs%'
"""

dag_list = "', '".join([f"DAG:{dag}" for dag in SPECIFIC_DAGS])
dag_query = f"""
SELECT pv.id as permission_view_id
FROM ab_permission_view pv
JOIN ab_view_menu vm ON pv.view_menu_id = vm.id
WHERE vm.name IN ('{dag_list}')
"""

non_dag_perms = spark.sql(non_dag_query)
dag_perms = spark.sql(dag_query)
all_perms = non_dag_perms.union(dag_perms)
pv_ids = [row.permission_view_id for row in all_perms.collect()]

print(f"Total permissions: {len(pv_ids)}")

# Check if role exists
existing_roles = spark.sql(f"SELECT id FROM ab_role WHERE name = '{TARGET_ROLE}'")
if existing_roles.count() > 0:
    raise Exception(f"Role '{TARGET_ROLE}' already exists. Delete it first.")

# Get next role ID
max_id_df = spark.sql("SELECT MAX(id) as max_id FROM ab_role")
max_id = max_id_df.first().max_id
new_role_id = max_id + 1 if max_id else 1

print(f"New role ID: {new_role_id}")

# Get JDBC connection details
glue_client = boto3.client('glue')
response = glue_client.get_connection(Name=CONNECTION_NAME)
props = response['Connection']['ConnectionProperties']
jdbc_url = props['JDBC_CONNECTION_URL']
username = props['USERNAME']
password = props['PASSWORD']

# Create role
new_role_data = [(new_role_id, TARGET_ROLE)]
new_role_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False)
])
new_role_df = spark.createDataFrame(new_role_data, schema=new_role_schema)

new_role_df.write.jdbc(
    url=jdbc_url,
    table="public.ab_role",
    mode="append",
    properties={"user": username, "password": password, "driver": "org.postgresql.Driver"}
)
print(f"✓ Role created")

# Create permissions
permission_data = [(pv_id, new_role_id) for pv_id in pv_ids]
permission_schema = StructType([
    StructField("permission_view_id", IntegerType(), False),
    StructField("role_id", IntegerType(), False)
])
permission_df = spark.createDataFrame(permission_data, schema=permission_schema)

permission_df.write.jdbc(
    url=jdbc_url,
    table="public.ab_permission_view_role",
    mode="append",
    properties={"user": username, "password": password, "driver": "org.postgresql.Driver"}
)
print(f"✓ Permissions assigned: {len(pv_ids)}")

job.commit()
print(f"✓ Role '{TARGET_ROLE}' created successfully!")
'''
    
    s3_client = boto3.client('s3', region_name=aws_region)
    script_key = f'glue-scripts/create_mwaa_role.py'
    
    try:
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=script_key,
            Body=glue_script.encode('utf-8'),
            ContentType='text/x-python'
        )
        script_location = f's3://{s3_bucket}/{script_key}'
        print(f"✓ Script uploaded to: {script_location}")
        return script_location
    except Exception as e:
        raise AirflowException(f"Failed to upload script: {e}")

# ============================================================================
# DAG Definition
# ============================================================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Create Glue job to create custom MWAA roles',
    schedule_interval=None,
    catchup=False,
    tags=['glue', 'rbac', 'admin'],
    params={
        "aws_region": "us-east-1",
        "stack_name": "{{VPC_STACK_NAME}}",  # Replaced by deploy script
        "source_role": "User",
        "target_role": "MWAARestrictedTest",
        "specific_dags": ["hello_world_advanced", "hello_world_simple"]
    }
) as dag:
    
    # Task 1: Create Glue connection
    glue_conn = create_glue_connection()
    
    # Task 2: Get Glue role ARN from stack
    glue_role = get_glue_role_arn()
    
    # Task 3: Get S3 bucket name from stack
    s3_bucket = get_s3_bucket_name()
    
    # Task 4: Upload script
    script_loc = upload_glue_script()
    
    # Task 5: Run Glue job
    @task()
    def run_glue_job_task(**context):
        """Execute the Glue job"""
        dag_conf = context['dag_run'].conf or {}
        aws_region = dag_conf.get('aws_region', 'us-east-1')
        source_role = dag_conf.get('source_role', 'User')
        target_role = dag_conf.get('target_role', 'MWAARestrictedTest')
        specific_dags = dag_conf.get('specific_dags', ['hello_world_advanced', 'hello_world_simple'])
        
        glue_conn_name = context['ti'].xcom_pull(task_ids='create_glue_connection')
        script_location = context['ti'].xcom_pull(task_ids='upload_glue_script')
        glue_role_arn = context['ti'].xcom_pull(task_ids='get_glue_role_arn')
        s3_bucket = context['ti'].xcom_pull(task_ids='get_s3_bucket_name')
        
        glue_client = boto3.client('glue', region_name=aws_region)
        glue_job_name = f"{ENV_NAME}_create_role"
        
        # Create or update Glue job
        job_config = {
            'Name': glue_job_name,
            'Description': 'Create custom MWAA role',
            'Role': glue_role_arn,
            'Command': {
                'Name': 'glueetl',
                'ScriptLocation': script_location,
                'PythonVersion': '3'
            },
            'DefaultArguments': {
                '--job-language': 'python',
                '--enable-metrics': 'true',
                '--enable-spark-ui': 'true',
                '--enable-continuous-cloudwatch-log': 'true',
                '--CONNECTION_NAME': glue_conn_name,
                '--SOURCE_ROLE': source_role,
                '--TARGET_ROLE': target_role,
                '--SPECIFIC_DAGS': json.dumps(specific_dags),
            },
            'MaxRetries': 0,
            'Timeout': 60,
            'GlueVersion': '4.0',
            'NumberOfWorkers': 2,
            'WorkerType': 'G.1X',
            'Connections': {
                'Connections': [glue_conn_name]
            }
        }
        
        try:
            glue_client.get_job(JobName=glue_job_name)
            # For update, remove the 'Name' field from job_config
            job_update_config = {k: v for k, v in job_config.items() if k != 'Name'}
            glue_client.update_job(JobName=glue_job_name, JobUpdate=job_update_config)
            print(f"✓ Updated Glue job: {glue_job_name}")
        except glue_client.exceptions.EntityNotFoundException:
            glue_client.create_job(**job_config)
            print(f"✓ Created Glue job: {glue_job_name}")
        
        # Start job run
        response = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments={
                '--CONNECTION_NAME': glue_conn_name,
                '--SOURCE_ROLE': source_role,
                '--TARGET_ROLE': target_role,
                '--SPECIFIC_DAGS': json.dumps(specific_dags),
            }
        )
        
        job_run_id = response['JobRunId']
        print(f"✓ Started Glue job run: {job_run_id}")
        
        # Wait for completion
        import time
        while True:
            response = glue_client.get_job_run(JobName=glue_job_name, RunId=job_run_id)
            status = response['JobRun']['JobRunState']
            
            if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
                break
            
            print(f"  Job status: {status}")
            time.sleep(30)
        
        if status == 'SUCCEEDED':
            print(f"✓ Glue job completed successfully!")
            print(f"✓ Role '{target_role}' created")
        else:
            error_msg = response['JobRun'].get('ErrorMessage', 'Unknown error')
            raise AirflowException(f"Glue job failed with status {status}: {error_msg}")
        
        return job_run_id
    
    run_job = run_glue_job_task()
    
    # Set dependencies - tasks must run sequentially
    glue_conn >> glue_role >> s3_bucket >> script_loc >> run_job

# Example trigger configuration:
# {
#   "aws_region": "us-east-1",
#   "stack_name": "{{VPC_STACK_NAME}}",  # Replaced by deploy script
#   "source_role": "User",
#   "target_role": "MWAARestrictedTest",
#   "specific_dags": ["hello_world_advanced", "hello_world_simple"]
# }
