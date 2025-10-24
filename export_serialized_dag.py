"""
Export Serialized DAG Data to S3
Exports serialized DAG data from the Airflow metadata database to S3 for analysis
Compatible with Apache Airflow v2.7+ and v3.10+
"""

import os
import json
import gzip
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.configuration import conf

# =============================================================================
# CONFIGURATION PARAMETERS (Parameterized via Airflow Variables)
# =============================================================================

# Core parameters
ENV_NAME = Variable.get(
    "export_dag_env_name", 
    default_var=os.getenv("AIRFLOW_ENV_NAME", "MyAirflowEnvironment")
)
S3_BUCKET = Variable.get(
    "export_dag_s3_bucket",
    default_var="aws-athena-query-results-us-east-1-515232103838",
)

# Export options
COMPRESS_EXPORTS = Variable.get("export_dag_compress", default_var="true").lower() == "true"
INCLUDE_METADATA = Variable.get("export_dag_include_metadata", default_var="true").lower() == "true"

# DAG configuration
DAG_ID = os.path.basename(__file__).replace(".py", "")

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# =============================================================================
# CORE EXPORT FUNCTIONS
# =============================================================================

@task()
def create_postgres_connection():
    """Create PostgreSQL connection from MWAA environment variables"""
    from airflow.models import Connection
    from airflow import settings

    # Try both possible environment variable names for compatibility
    sql_alchemy_conn = (
        os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN") or 
        os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    )

    if sql_alchemy_conn is None:
        raise ValueError("SQL_ALCHEMY_CONN environment variable is not set.")

    # Parse connection string
    s = sql_alchemy_conn.split("@")
    s2 = s[1].split("?")[0]
    host_port_db = s2.split("/")
    host_port = host_port_db[0]
    database = host_port_db[1] if len(host_port_db) > 1 else "airflow"

    if ":" in host_port:
        host = host_port.split(":")[0]
        port = int(host_port.split(":")[1])
    else:
        host = host_port
        port = 5432

    c = s[0].split("//")[1].split(":")
    username = c[0]
    password = c[1]

    env_name_lower = ENV_NAME.lower().replace(" ", "_")
    conn_id = f"{env_name_lower}_postgres_export"

    session = settings.Session()
    existing_conn = (
        session.query(Connection).filter(Connection.conn_id == conn_id).first()
    )

    if existing_conn:
        session.close()
        return {"conn_id": conn_id, "status": "exists"}

    try:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type="postgres",
            host=host,
            port=port,
            schema=database,
            login=username,
            password=password,
            description=f"MWAA PostgreSQL export connection for {ENV_NAME}",
        )
        session.add(new_conn)
        session.commit()
        print(f"Created PostgreSQL connection: {conn_id}")
        status = "created"
    except Exception as e:
        session.rollback()
        print(f"Failed to create connection: {e}")
        status = "failed"
        raise
    finally:
        session.close()

    return {"conn_id": conn_id, "status": status}


@task()
def validate_and_prepare_dag_list(**context):
    """Validate DAG names and prepare export list"""
    
    # Get DAG names from parameters
    dag_run = context.get("dag_run")
    params = context.get("params", {})
    
    if dag_run and dag_run.conf and "dag_names" in dag_run.conf:
        dag_names_param = dag_run.conf["dag_names"]
    else:
        dag_names_param = params.get("dag_names", "")
    
    if not dag_names_param:
        raise ValueError("dag_names parameter is required. Provide comma-separated DAG names.")
    
    # Parse comma-separated DAG names
    dag_names = [name.strip() for name in dag_names_param.split(",") if name.strip()]
    
    if not dag_names:
        raise ValueError("No valid DAG names provided.")
    
    print(f"\n" + "=" * 80)
    print("DAG EXPORT PREPARATION")
    print("=" * 80)
    print(f"Requested DAGs for export: {len(dag_names)}")
    for i, dag_name in enumerate(dag_names, 1):
        print(f"  {i}. {dag_name}")
    
    # Get connection info
    conn_info = context["task_instance"].xcom_pull(task_ids="create_postgres_connection")
    conn_id = conn_info["conn_id"]
    
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)
    
    # Validate DAG names exist in database
    print(f"\n📋 VALIDATING DAG EXISTENCE")
    print("-" * 40)
    
    # Create parameterized query for safety
    placeholders = ",".join(["%s"] * len(dag_names))
    validation_query = f"""
    SELECT dag_id, 
           LENGTH(data::text) as size_bytes,
           ROUND(LENGTH(data::text) / 1048576.0, 2) as size_mb,
           last_updated,
           dag_hash
    FROM serialized_dag 
    WHERE dag_id IN ({placeholders})
    ORDER BY dag_id
    """
    
    existing_dags = postgres_hook.get_records(validation_query, parameters=dag_names)
    
    if not existing_dags:
        raise ValueError(f"None of the specified DAGs found in database: {dag_names}")
    
    # Create validation results
    found_dag_ids = {record[0] for record in existing_dags}
    missing_dags = [dag for dag in dag_names if dag not in found_dag_ids]
    
    print(f"Found DAGs: {len(found_dag_ids)}")
    print(f"Missing DAGs: {len(missing_dags)}")
    
    # Display found DAGs with sizes
    print(f"\n✅ FOUND DAGs:")
    print(f"{'DAG ID':<40} {'Size (MB)':<10} {'Last Updated':<20}")
    print("-" * 75)
    
    total_size_mb = 0
    for record in existing_dags:
        dag_id = record[0]
        size_mb = float(record[2])
        last_updated = str(record[3])[:19]  # Truncate timestamp
        total_size_mb += size_mb
        
        print(f"{dag_id:<40} {size_mb:<10.2f} {last_updated:<20}")
    
    if missing_dags:
        print(f"\n❌ MISSING DAGs:")
        for dag in missing_dags:
            print(f"  - {dag}")
    
    print(f"\n📊 EXPORT SUMMARY:")
    print(f"Total DAGs to export: {len(found_dag_ids)}")
    print(f"Total serialized data: {total_size_mb:.2f} MB")
    print(f"Compression enabled: {COMPRESS_EXPORTS}")
    print(f"Include metadata: {INCLUDE_METADATA}")
    
    return {
        "requested_dags": dag_names,
        "found_dags": list(found_dag_ids),
        "missing_dags": missing_dags,
        "dag_details": [
            {
                "dag_id": record[0],
                "size_bytes": record[1],
                "size_mb": float(record[2]),
                "last_updated": str(record[3]),
                "dag_hash": record[4]
            }
            for record in existing_dags
        ],
        "total_size_mb": total_size_mb
    }


@task()
def export_serialized_dags(**context):
    """Export serialized DAG data to S3"""
    
    # Get validation results
    validation_result = context["task_instance"].xcom_pull(task_ids="validate_and_prepare_dag_list")
    found_dags = validation_result["found_dags"]
    
    if not found_dags:
        raise ValueError("No DAGs to export")
    
    # Get runtime parameters
    dag_run = context.get("dag_run")
    params = context.get("params", {})
    
    if dag_run and dag_run.conf:
        env_name = dag_run.conf.get("env_name", params.get("env_name", ENV_NAME))
        s3_bucket = dag_run.conf.get("s3_bucket", params.get("s3_bucket", S3_BUCKET))
    else:
        env_name = params.get("env_name", ENV_NAME)
        s3_bucket = params.get("s3_bucket", S3_BUCKET)
    
    print(f"\n" + "=" * 80)
    print("EXPORTING SERIALIZED DAGs TO S3")
    print("=" * 80)
    print(f"Environment: {env_name}")
    print(f"S3 Bucket: {s3_bucket}")
    print(f"DAGs to export: {len(found_dags)}")
    
    # Get connection info
    conn_info = context["task_instance"].xcom_pull(task_ids="create_postgres_connection")
    conn_id = conn_info["conn_id"]
    
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)
    
    # Export each DAG
    export_results = []
    
    for dag_id in found_dags:
        try:
            print(f"\n📤 Exporting: {dag_id}")
            
            # Get full DAG data
            export_query = """
            SELECT 
                dag_id,
                fileloc,
                data,
                data_compressed,
                last_updated,
                dag_hash,
                processor_subdir
            FROM serialized_dag 
            WHERE dag_id = %s
            """
            
            dag_records = postgres_hook.get_records(export_query, parameters=[dag_id])
            
            if not dag_records:
                print(f"  ❌ DAG {dag_id} not found during export")
                continue
            
            record = dag_records[0]
            
            # Prepare export data
            export_data = {
                "export_metadata": {
                    "exported_at": datetime.now().isoformat(),
                    "exported_by": "export_serialized_dag",
                    "source_environment": env_name,
                    "airflow_version": get_airflow_version()
                },
                "dag_metadata": {
                    "dag_id": record[0],
                    "fileloc": record[1],
                    "last_updated": record[4].isoformat() if record[4] else None,
                    "dag_hash": record[5],
                    "processor_subdir": record[6],
                    "serialized_size_bytes": len(str(record[2])) if record[2] else 0,
                    "has_compressed_data": record[3] is not None and len(record[3]) > 0
                },
                "serialized_data": record[2]  # The actual JSON data
            }
            
            # Optionally exclude metadata to save space
            if not INCLUDE_METADATA:
                export_data = {
                    "dag_id": record[0],
                    "serialized_data": record[2]
                }
            
            # Convert to JSON string
            json_data = json.dumps(export_data, indent=2, default=str)
            
            # Prepare for S3 upload
            file_content = json_data.encode('utf-8')
            file_extension = ".json"
            
            # Compress if enabled
            if COMPRESS_EXPORTS:
                file_content = gzip.compress(file_content)
                file_extension = ".json.gz"
            
            # Generate S3 key
            execution_date = context["ds"]
            timestamp = context["ts"].replace(":", "-").replace("+", "_")
            
            s3_key = f"mwaa_diagnostics/{env_name}/{execution_date}/exported_dags/{dag_id}_{timestamp}{file_extension}"
            
            # Upload to S3
            s3_result = upload_to_s3(file_content, s3_bucket, s3_key, dag_id)
            
            export_results.append({
                "dag_id": dag_id,
                "s3_location": s3_result["location"],
                "file_size_bytes": len(file_content),
                "compressed": COMPRESS_EXPORTS,
                "status": "success"
            })
            
            print(f"  ✅ Exported to: {s3_result['location']}")
            print(f"  📁 File size: {len(file_content):,} bytes")
            
        except Exception as e:
            print(f"  ❌ Failed to export {dag_id}: {str(e)}")
            export_results.append({
                "dag_id": dag_id,
                "status": "failed",
                "error": str(e)
            })
    
    # Generate summary report
    successful_exports = [r for r in export_results if r["status"] == "success"]
    failed_exports = [r for r in export_results if r["status"] == "failed"]
    
    print(f"\n📊 EXPORT SUMMARY")
    print("-" * 40)
    print(f"Total DAGs processed: {len(export_results)}")
    print(f"Successful exports: {len(successful_exports)}")
    print(f"Failed exports: {len(failed_exports)}")
    
    if successful_exports:
        total_size = sum(r["file_size_bytes"] for r in successful_exports)
        print(f"Total exported size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
    
    if failed_exports:
        print(f"\n❌ FAILED EXPORTS:")
        for failure in failed_exports:
            print(f"  - {failure['dag_id']}: {failure['error']}")
    
    # Create and upload summary report
    summary_report = create_export_summary_report(
        validation_result, export_results, env_name, context
    )
    
    summary_s3_key = f"mwaa_diagnostics/{env_name}/{execution_date}/exported_dags/_export_summary_{timestamp}.txt"
    summary_upload = upload_to_s3(
        summary_report.encode('utf-8'), s3_bucket, summary_s3_key, "summary"
    )
    
    print(f"\n📋 Summary report: {summary_upload['location']}")
    
    return {
        "export_results": export_results,
        "successful_count": len(successful_exports),
        "failed_count": len(failed_exports),
        "summary_location": summary_upload["location"]
    }


def get_airflow_version() -> str:
    """Get Airflow version"""
    try:
        import airflow
        return airflow.__version__
    except Exception:
        return "Unknown"


def upload_to_s3(content: bytes, bucket: str, key: str, dag_id: str) -> Dict[str, Any]:
    """Upload content to S3"""
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        s3_hook = S3Hook(aws_conn_id="aws_default")
        
        # Upload content
        s3_hook.load_bytes(
            bytes_data=content,
            key=key,
            bucket_name=bucket,
            replace=True
        )
        
        s3_location = f"s3://{bucket}/{key}"
        
        return {
            "status": "success",
            "location": s3_location,
            "bucket": bucket,
            "key": key
        }
        
    except Exception as e:
        raise Exception(f"Failed to upload {dag_id} to S3: {str(e)}")


def create_export_summary_report(
    validation_result: Dict[str, Any], 
    export_results: List[Dict[str, Any]], 
    env_name: str,
    context: Dict[str, Any]
) -> str:
    """Create a summary report of the export operation"""
    
    lines = []
    lines.append("=" * 80)
    lines.append("SERIALIZED DAG EXPORT SUMMARY")
    lines.append("=" * 80)
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append(f"Environment: {env_name}")
    lines.append(f"Airflow Version: {get_airflow_version()}")
    lines.append("")
    
    # Export request details
    lines.append("📋 EXPORT REQUEST")
    lines.append("-" * 40)
    lines.append(f"Requested DAGs: {len(validation_result['requested_dags'])}")
    lines.append(f"Found in database: {len(validation_result['found_dags'])}")
    lines.append(f"Missing from database: {len(validation_result['missing_dags'])}")
    lines.append(f"Compression enabled: {COMPRESS_EXPORTS}")
    lines.append(f"Include metadata: {INCLUDE_METADATA}")
    lines.append("")
    
    # DAG details
    lines.append("📊 DAG DETAILS")
    lines.append("-" * 40)
    lines.append(f"{'DAG ID':<40} {'Size (MB)':<10} {'Status':<15}")
    lines.append("-" * 70)
    
    for dag_detail in validation_result["dag_details"]:
        dag_id = dag_detail["dag_id"]
        size_mb = dag_detail["size_mb"]
        
        # Find export result
        export_result = next((r for r in export_results if r["dag_id"] == dag_id), None)
        status = "✅ Exported" if export_result and export_result["status"] == "success" else "❌ Failed"
        
        lines.append(f"{dag_id:<40} {size_mb:<10.2f} {status:<15}")
    
    # Export results
    successful_exports = [r for r in export_results if r["status"] == "success"]
    failed_exports = [r for r in export_results if r["status"] == "failed"]
    
    lines.append("")
    lines.append("📤 EXPORT RESULTS")
    lines.append("-" * 40)
    lines.append(f"Total processed: {len(export_results)}")
    lines.append(f"Successful: {len(successful_exports)}")
    lines.append(f"Failed: {len(failed_exports)}")
    
    if successful_exports:
        total_size = sum(r["file_size_bytes"] for r in successful_exports)
        lines.append(f"Total exported size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
        
        lines.append("")
        lines.append("✅ SUCCESSFUL EXPORTS:")
        for result in successful_exports:
            lines.append(f"  - {result['dag_id']}")
            lines.append(f"    Location: {result['s3_location']}")
            lines.append(f"    Size: {result['file_size_bytes']:,} bytes")
    
    if failed_exports:
        lines.append("")
        lines.append("❌ FAILED EXPORTS:")
        for result in failed_exports:
            lines.append(f"  - {result['dag_id']}: {result['error']}")
    
    if validation_result["missing_dags"]:
        lines.append("")
        lines.append("🔍 MISSING DAGs (not found in database):")
        for dag in validation_result["missing_dags"]:
            lines.append(f"  - {dag}")
    
    lines.append("")
    lines.append("=" * 80)
    
    return "\n".join(lines)


@task()
def cleanup_postgres_connection(**context):
    """Clean up the export connection"""
    from airflow.models import Connection
    from airflow import settings
    
    conn_info = context["task_instance"].xcom_pull(task_ids="create_postgres_connection")
    conn_id = conn_info["conn_id"]
    
    session = settings.Session()
    try:
        conn_to_delete = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if conn_to_delete:
            session.delete(conn_to_delete)
            session.commit()
            print(f"Deleted export connection: {conn_id}")
        return {"status": "deleted"}
    except Exception as e:
        session.rollback()
        print(f"Failed to delete connection: {e}")
        return {"status": "failed"}
    finally:
        session.close()


# =============================================================================
# DAG DEFINITION
# =============================================================================

@dag(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Export serialized DAG data from database to S3 for analysis",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["export", "serialization", "s3", "backup"],
    params={
        "dag_names": "",  # Comma-separated list of DAG names
        "env_name": ENV_NAME,
        "s3_bucket": S3_BUCKET
    },
    doc_md="""
    ## Export Serialized DAG Data
    
    This DAG exports serialized DAG data from the Airflow metadata database to S3.
    
    ### Parameters:
    - **dag_names**: Comma-separated list of DAG names to export (required)
    - **env_name**: Environment name for S3 folder structure (default: from Variable)
    - **s3_bucket**: S3 bucket for exports (default: from Variable)
    
    ### Usage:
    Trigger with DAG names:
    ```json
    {
        "dag_names": "dag1,dag2,dag3",
        "env_name": "Production",
        "s3_bucket": "my-export-bucket"
    }
    ```
    
    ### Output Structure:
    ```
    s3://bucket/mwaa_diagnostics/{env_name}/{date}/exported_dags/
    ├── dag1_timestamp.json.gz
    ├── dag2_timestamp.json.gz
    └── _export_summary_timestamp.txt
    ```
    """,
)
def export_serialized_dag_workflow():
    """Export serialized DAG data workflow"""
    
    # Setup
    conn_setup = create_postgres_connection()
    
    # Validate and prepare DAG list
    validation = validate_and_prepare_dag_list()
    
    # Export DAGs to S3
    export = export_serialized_dags()
    
    # Cleanup
    cleanup = cleanup_postgres_connection()
    
    # Set dependencies
    conn_setup >> validation >> export >> cleanup


# Instantiate the DAG
export_serialized_dag_instance = export_serialized_dag_workflow()