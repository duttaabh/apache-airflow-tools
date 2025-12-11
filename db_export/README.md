# MWAA Metadata Export Solution

This solution provides automated backup of Amazon MWAA (Managed Workflows for Apache Airflow) metadata database using AWS Glue. It consists of an Airflow DAGs for Airflow 2.x and 3.x that orchestrates AWS Glue jobs to export metadata tables to S3 in a date-organized structure.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   MWAA DAG      │───▶│   AWS Glue      │───▶│   S3 Bucket     │
│ (Orchestrator)  │    │ (Export Job)    │    │ (Backup Store)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
    ┌────▼────┐             ┌────▼────┐             ┌────▼────┐
    │ Creates │             │ Connects│             │ Stores  │
    │ Glue    │             │ to MWAA │             │ in Date │
    │ Connection│           │ MetaDB  │             │ Folders │
    └─────────┘             └─────────┘             └─────────┘
```

## Components

### 1. Airflow DAG (`glue_mwaa_export_v3.py`)
- **Purpose**: Orchestrates the metadata export process
- **Runtime**: Runs on Amazon MWAA (Airflow 3.x)
- **Functions**:
  - Creates AWS Glue connection to MWAA metadata database
  - Launches AWS Glue job for data export
  - Handles both Airflow 2.x and 3.x connection methods

### 2. AWS Glue Script (`mwaa_metadb_export.py`)
- **Purpose**: Performs the actual data extraction and export
- **Runtime**: Runs on AWS Glue workers
- **Functions**:
  - Connects to MWAA PostgreSQL metadata database
  - Exports 20+ Airflow tables with date filtering
  - Compresses and stores data in S3 with date-organized structure

## Output Structure

Exports are organized by date for easy management:

```
s3://your-bucket/exports/your-env-backups/
├── 2024/
│   ├── 12/
│   │   ├── 11/
│   │   │   ├── dag_run_143022.csv.gz
│   │   │   ├── task_instance_143023.csv.gz
│   │   │   ├── xcom_143024.csv.gz
│   │   │   ├── ...
│   │   │   └── export_summary_143025.json.gz
│   │   └── 12/
│   └── 01/
└── 2025/
```

## Exported Tables

The solution exports 20+ Airflow 3.0 metadata tables:

### Core Execution Data
- `dag_run` - DAG execution instances
- `task_instance` - Task execution instances  
- `xcom` - Cross-communication between tasks
- `task_instance_history` - Historical task data

### Configuration & Metadata
- `dag` - DAG definitions
- `dag_code` - DAG source code
- `variable` - Airflow variables
- `connection` - Connection definitions
- `slot_pool` - Resource pools

### Logging & Monitoring
- `log` - System logs
- `import_error` - DAG parsing errors
- `job` - Airflow jobs (scheduler, webserver)

### New Airflow 3.0 Features
- `asset_event` - Asset/Dataset events
- `backfill` - Backfill jobs
- `dag_version` - DAG versioning
- `hitl_detail` - Human-in-the-loop requests

## Setup Instructions

### 1. Deploy Files

Upload the files to your S3 bucket:
```bash
# Upload Glue script
aws s3 cp files/scripts/mwaa_metadb_export.py s3://your-bucket/scripts/

# Upload DAG to MWAA
aws s3 cp files/dags/glue_mwaa_export_v3.py s3://your-mwaa-bucket/dags/
```

### 2. Configure Airflow Variables

Set these variables in your MWAA environment:
```python
# Required variables
aws_region = "us-east-1"  # Your AWS region
glue_role_name = "your-glue-execution-role"  # Glue service role
export_s3_bucket = "your-backup-bucket"  # S3 bucket for exports

# Optional variables  
database_name = "airflow_metadb"  # Database name (for logging)
```

### 3. IAM Permissions Setup

## IAM Permissions Required

### MWAA Execution Role Permissions

Add these permissions to your MWAA execution role (replace placeholders with actual values):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "GlueJobManagement",
            "Effect": "Allow",
            "Action": [
                "glue:CreateJob",
                "glue:GetJob",
                "glue:StartJobRun",
                "glue:GetJobRun"
            ],
            "Resource": [
                "arn:aws:glue:us-east-1:123456789012:job/${ENV_NAME}_export"
            ]
        },
        {
            "Sid": "GlueConnectionManagement",
            "Effect": "Allow",
            "Action": [
                "glue:CreateConnection",
                "glue:GetConnection"
            ],
            "Resource": [
                "arn:aws:glue:us-east-1:123456789012:connection/${ENV_NAME}_conn"
            ]
        },
        {
            "Sid": "MWAAEnvironmentAccess",
            "Effect": "Allow",
            "Action": [
                "airflow:GetEnvironment"
            ],
            "Resource": "arn:aws:airflow:us-east-1:123456789012:environment/${ENV_NAME}"
        },
        {
            "Sid": "EC2NetworkDescribe",
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeSubnets",
                "ec2:DescribeSecurityGroups"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "ec2:Region": "us-east-1"
                }
            }
        },
        {
            "Sid": "S3ScriptAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::your-backup-bucket/scripts/mwaa_metadb_export.py"
        },
        {
            "Sid": "IAMPassRole",
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": "arn:aws:iam::123456789012:role/your-glue-execution-role",
            "Condition": {
                "StringEquals": {
                    "iam:PassedToService": "glue.amazonaws.com"
                }
            }
        }
    ]
}
```

### AWS Glue Execution Role Permissions

Create a separate IAM role for Glue with these permissions (replace placeholders with actual values):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "EC2NetworkInterfaceManagement",
            "Effect": "Allow",
            "Action": [
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSubnets",
                "ec2:DescribeVpcAttribute"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "ec2:Region": "us-east-1"
                }
            }
        },
        {
            "Sid": "EC2NetworkInterfaceTagging",
            "Effect": "Allow",
            "Action": [
                "ec2:CreateTags",
                "ec2:DeleteTags"
            ],
            "Resource": "arn:aws:ec2:us-east-1:123456789012:network-interface/*",
            "Condition": {
                "StringEquals": {
                    "ec2:CreateAction": "CreateNetworkInterface"
                }
            }
        },
        {
            "Sid": "S3ScriptRead",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::your-backup-bucket/scripts/mwaa_metadb_export.py"
        },
        {
            "Sid": "S3ExportWrite",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectAcl"
            ],
            "Resource": "arn:aws:s3:::your-backup-bucket/exports/*"
        },
        {
            "Sid": "S3BucketList",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": "arn:aws:s3:::your-backup-bucket",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "scripts/*",
                        "exports/*"
                    ]
                }
            }
        },
        {
            "Sid": "CloudWatchLogs",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:us-east-1:123456789012:log-group:/aws-glue/jobs/logs-v2",
                "arn:aws:logs:us-east-1:123456789012:log-group:/aws-glue/jobs/logs-v2:*"
            ]
        },
        {
            "Sid": "CloudWatchMetrics",
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "cloudwatch:namespace": "AWS/Glue"
                }
            }
        }
    ]
}
```

**Trust Policy for Glue Role:**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```

## Configuration Options

### Data Retention
Modify `MAX_AGE_IN_DAYS` in the DAG to control how much historical data to export:
```python
MAX_AGE_IN_DAYS = 30  # Export last 30 days of data
```

### Table Selection
Customize `OBJECTS_TO_EXPORT` to include/exclude specific tables:
```python
OBJECTS_TO_EXPORT = [
    {"table": "dag_run", "date_field": "execution_date"},
    {"table": "task_instance", "date_field": "start_date"},
    # Add or remove tables as needed
]
```

### Glue Job Configuration
Adjust Glue job settings in the DAG:
```python
create_job_kwargs={
    'NumberOfWorkers': 2,        # Increase for larger datasets
    'WorkerType': 'G.1X',        # Use larger worker types if needed
    'GlueVersion': '4.0',        # Latest Glue version
    'MaxRetries': 1,             # Job retry attempts
    'Timeout': 60,               # Job timeout in minutes
}
```

## Monitoring & Troubleshooting

### Monitoring
- **Airflow UI**: Monitor DAG execution in MWAA console
- **Glue Console**: Track job runs and view logs
- **CloudWatch**: View detailed execution logs
- **S3**: Verify export files and summary reports

### Common Issues

1. **Connection Failures**
   - Verify MWAA security groups allow Glue access
   - Check subnet routing and VPC endpoints
   - Ensure database credentials are accessible

2. **Permission Errors**
   - Verify IAM roles have required permissions
   - Check S3 bucket policies
   - Ensure PassRole permissions are correct

3. **Table Not Found Errors**
   - Some tables may not exist in all MWAA versions
   - Script gracefully skips missing tables
   - Check export summary for details

### Debugging
Enable detailed logging by checking Glue job logs in CloudWatch:
```
/aws-glue/jobs/logs-v2/
```

## Security Considerations

1. **Database Access**: Glue connects to MWAA metadata database using VPC networking
2. **Encryption**: All exports are stored in S3 with server-side encryption
3. **Access Control**: Use IAM policies to restrict access to backup data
4. **Network Security**: Glue runs in MWAA VPC with security group restrictions

## Cost Optimization

1. **S3 Lifecycle Policies**: Automatically delete old backups
2. **Glue Job Sizing**: Use appropriate worker types and counts
3. **Compression**: All exports are gzipped to reduce storage costs
4. **Scheduling**: Run exports during off-peak hours

## Example S3 Lifecycle Policy

```json
{
    "Rules": [
        {
            "ID": "MWAABackupRetention",
            "Status": "Enabled",
            "Filter": {
                "Prefix": "exports/"
            },
            "Transitions": [
                {
                    "Days": 30,
                    "StorageClass": "STANDARD_IA"
                },
                {
                    "Days": 90,
                    "StorageClass": "GLACIER"
                }
            ],
            "Expiration": {
                "Days": 365
            }
        }
    ]
}
```

## Support

For issues or questions:
1. Check CloudWatch logs for detailed error messages
2. Verify IAM permissions and network connectivity
3. Review MWAA and Glue service limits
4. Consult AWS documentation for service-specific guidance
