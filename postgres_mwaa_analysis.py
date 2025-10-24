import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

## PostgreSQL Direct Query Settings

# Analysis time window (days)
ANALYSIS_DAYS = Variable.get("analysis_days", default_var=7)

# S3 bucket for saving results (optional)
S3_BUCKET = Variable.get(
    "results_s3_bucket", "aws-athena-query-results-us-east-1-515232103838"
)

# Do not edit below this line
ENV_NAME = os.getenv("AIRFLOW_ENV_NAME") or Variable.get(
    "mwaa_env_name", default_var="MyAirflowEnvironment111"
)
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


@task()
def create_postgres_connection():
    """
    Create PostgreSQL connection from MWAA environment variables
    """
    from airflow.models import Connection
    from airflow import settings

    sql_alchemy_conn = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
    if sql_alchemy_conn is None:
        sql_alchemy_conn = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")

    if sql_alchemy_conn is None:
        raise ValueError("SQL_ALCHEMY_CONN environment variable is not set.")

    print(f"Connection string: ", sql_alchemy_conn)
    # Parse connection string using the same logic as glue_mwaa_export_v2.py
    # Format: postgresql://user:pass@host:port/dbname
    s = sql_alchemy_conn.split("@")
    s2 = s[1].split("?")[0]  # Remove query parameters if any

    # Extract host, port, and database
    host_port_db = s2.split("/")
    host_port = host_port_db[0]
    database = host_port_db[1] if len(host_port_db) > 1 else "airflow"

    # Extract host and port
    if ":" in host_port:
        host = host_port.split(":")[0]
        port = int(host_port.split(":")[1])
    else:
        host = host_port
        port = 5432

    # Extract username and password
    c = s[0].split("//")[1].split(":")
    username = c[0]
    password = c[1]

    # Create connection ID with environment name (lowercase)
    env_name_lower = ENV_NAME.lower() if ENV_NAME else "mwaa"
    conn_id = f"{env_name_lower}_postgres"

    print(f"Parsed connection details:")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    print(f"  Database: {database}")
    print(f"  Username: {username}")
    print(f"  Connection ID: {conn_id}")

    # Check if connection already exists
    session = settings.Session()
    existing_conn = (
        session.query(Connection).filter(Connection.conn_id == conn_id).first()
    )

    if existing_conn:
        print(f"PostgreSQL connection already exists: {conn_id}")
        session.close()
        return {
            "conn_id": conn_id,
            "host": host,
            "port": port,
            "database": database,
            "username": username,
            "status": "exists",
        }

    # Create new connection
    try:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type="postgres",
            host=host,
            port=port,
            schema=database,
            login=username,
            password=password,
            description=f"MWAA PostgreSQL connection for {ENV_NAME}",
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

    return {
        "conn_id": conn_id,
        "host": host,
        "port": port,
        "database": database,
        "username": username,
        "status": status,
    }


@task()
def analyze_serialized_dags(**context):
    """
    Analyze serialized DAG sizes and complexity using direct PostgreSQL queries
    """
    # Get connection info from previous task
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    # First, let's check the schema to understand data types
    schema_query = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns 
    WHERE table_name = 'serialized_dag'
    ORDER BY ordinal_position
    """

    try:
        schema_records = postgres_hook.get_records(schema_query)
        print(f"\\n=== SERIALIZED_DAG TABLE SCHEMA ===")
        for record in schema_records:
            print(f"record: {record}")
            print(
                f"{record[0]}: {record[1]} ({'NULL' if record[2] == 'YES' else 'NOT NULL'})"
            )
    except Exception as e:
        print(f"Could not retrieve schema: {e}")

    # Test query to see what data looks like
    test_query = """
    SELECT 
        dag_id,
        data_compressed,
        LENGTH(data::text) as raw_data_length,
        pg_typeof(data) as data_type,
        pg_typeof(data_compressed) as compressed_type
    FROM serialized_dag 
    LIMIT 3
    """

    try:
        test_records = postgres_hook.get_records(test_query)
        print(f"\\n=== SAMPLE DATA ===")
        for record in test_records:
            print(
                f"DAG: {record[0]}, Compressed: {record[1]}, Length: {record[2]}, Data Type: {record[3]}, Compressed Type: {record[4]}"
            )
    except Exception as e:
        print(f"Could not retrieve sample data: {e}")

    # Query for serialized DAG analysis - Fixed for JSON data type
    query = f"""
    SELECT 
        dag_id,
        fileloc,
        CASE 
            WHEN data_compressed IS NOT NULL AND LENGTH(data_compressed) > 0 THEN 'Compressed'
            ELSE 'Uncompressed'
        END as compression_status,
        LENGTH(data::text) as serialized_size_bytes,
        ROUND(LENGTH(data::text) / 1024.0, 2) as serialized_size_kb,
        ROUND(LENGTH(data::text) / 1048576.0, 2) as serialized_size_mb,
        last_updated,
        EXTRACT(DAY FROM (CURRENT_TIMESTAMP - last_updated)) as days_since_update,
        dag_hash,
        processor_subdir,
        -- Estimate task count from JSON data
        CASE 
            WHEN data::text LIKE '%"task_id"%' THEN 
                (LENGTH(data::text) - LENGTH(REPLACE(data::text, '"task_id"', ''))) / LENGTH('"task_id"')
            ELSE 0
        END as estimated_task_count,
        -- Check for common operators in JSON
        CASE 
            WHEN data::text LIKE '%BashOperator%' THEN 'BashOperator'
            WHEN data::text LIKE '%PythonOperator%' THEN 'PythonOperator'
            WHEN data::text LIKE '%S3%' THEN 'S3Operations'
            WHEN data::text LIKE '%Glue%' THEN 'GlueOperator'
            ELSE 'Other'
        END as primary_operator_type,
        -- Check for complexity indicators in JSON
        CASE 
            WHEN data::text LIKE '%depends_on_past%' THEN 'Has Dependencies'
            WHEN data::text LIKE '%retries%' THEN 'Has Retries'
            WHEN data::text LIKE '%pool%' THEN 'Uses Pools'
            ELSE 'Standard'
        END as complexity_indicators
    FROM serialized_dag
    WHERE data IS NOT NULL
    ORDER BY LENGTH(data::text) DESC
    """

    # Execute query and get results as list of tuples
    try:
        records = postgres_hook.get_records(query)
    except Exception as e:
        print(f"Primary query failed: {e}")
        print("Trying simplified query without text conversion...")

        # Fallback query - handle JSON data type
        simple_query = """
        SELECT 
            dag_id,
            fileloc,
            data_compressed as compression_status,
            LENGTH(data::text) as serialized_size_bytes,
            ROUND(LENGTH(data::text) / 1024.0, 2) as serialized_size_kb,
            ROUND(LENGTH(data::text) / 1048576.0, 2) as serialized_size_mb,
            last_updated,
            EXTRACT(DAY FROM (CURRENT_TIMESTAMP - last_updated)) as days_since_update,
            dag_hash,
            processor_subdir,
            -- Estimate task count from JSON data
            CASE 
                WHEN data::text LIKE '%"task_id"%' THEN 
                    (LENGTH(data::text) - LENGTH(REPLACE(data::text, '"task_id"', ''))) / LENGTH('"task_id"')
                ELSE 0
            END as estimated_task_count,
            -- Check for common operators in JSON
            CASE 
                WHEN data::text LIKE '%BashOperator%' THEN 'BashOperator'
                WHEN data::text LIKE '%PythonOperator%' THEN 'PythonOperator'
                WHEN data::text LIKE '%S3%' THEN 'S3Operations'
                WHEN data::text LIKE '%Glue%' THEN 'GlueOperator'
                ELSE 'Other'
            END as primary_operator_type,
            -- Check for complexity indicators in JSON
            CASE 
                WHEN data::text LIKE '%depends_on_past%' THEN 'Has Dependencies'
                WHEN data::text LIKE '%retries%' THEN 'Has Retries'
                WHEN data::text LIKE '%pool%' THEN 'Uses Pools'
                ELSE 'Standard'
            END as complexity_indicators
        FROM serialized_dag
        WHERE data IS NOT NULL
        ORDER BY LENGTH(data::text) DESC
        """

        records = postgres_hook.get_records(simple_query)

    if not records:
        print("No serialized DAG data found!")
        return {"total_dags": 0, "message": "No data available"}

    # Process results manually without pandas
    total_dags = len(records)
    size_kb_values = [
        float(record[4]) for record in records
    ]  # serialized_size_kb column
    size_mb_values = [
        float(record[5]) for record in records
    ]  # serialized_size_mb column

    # Calculate statistics
    avg_size_kb = sum(size_kb_values) / len(size_kb_values)
    max_size_mb = max(size_mb_values)
    min_size_kb = min(size_kb_values)

    # Print summary statistics
    print(f"\\n=== SERIALIZED DAG ANALYSIS RESULTS ===")
    print(f"Total DAGs analyzed: {total_dags}")
    print(f"Average DAG size: {avg_size_kb:.2f} KB")
    print(f"Largest DAG: {max_size_mb:.2f} MB")
    print(f"Smallest DAG: {min_size_kb:.2f} KB")

    # Top 10 largest DAGs
    print(f"\\n=== TOP 10 LARGEST DAGs ===")
    print(
        f"{'DAG ID':<30} {'Size (MB)':<10} {'Compression':<15} {'Est. Tasks':<10} {'Primary Op':<15}"
    )
    print("-" * 90)

    top_10_records = []
    for i, record in enumerate(records[:10]):
        dag_id = record[0] or "Unknown"
        size_mb = record[5] or 0
        compression = str(record[2]) if record[2] is not None else "Unknown"
        task_count = record[10] or 0
        operator_type = record[11] or "Unknown"

        print(
            f"{dag_id:<30} {float(size_mb):<10.2f} {compression:<15} {int(task_count):<10} {operator_type:<15}"
        )

        top_10_records.append(
            {
                "dag_id": dag_id,
                "serialized_size_mb": float(size_mb),
                "compression_status": compression,
                "estimated_task_count": int(task_count),
                "primary_operator_type": operator_type,
            }
        )

    # Compression analysis - handle bytea data_compressed field
    compressed_count = 0
    uncompressed_count = 0
    compressed_sizes = []
    uncompressed_sizes = []

    for record in records:
        size_kb = float(record[4]) if record[4] is not None else 0
        # record[2] is the raw data_compressed bytea field
        if record[2] is not None and len(record[2]) > 0:
            compressed_count += 1
            compressed_sizes.append(size_kb)
        else:
            uncompressed_count += 1
            uncompressed_sizes.append(size_kb)

    print(f"\\n=== COMPRESSION ANALYSIS ===")
    print(f"Compressed DAGs: {compressed_count}")
    if compressed_sizes:
        print(f"  - Average size: {sum(compressed_sizes)/len(compressed_sizes):.2f} KB")
        print(f"  - Max size: {max(compressed_sizes):.2f} KB")

    print(f"Uncompressed DAGs: {uncompressed_count}")
    if uncompressed_sizes:
        print(
            f"  - Average size: {sum(uncompressed_sizes)/len(uncompressed_sizes):.2f} KB"
        )
        print(f"  - Max size: {max(uncompressed_sizes):.2f} KB")

    # Size distribution
    size_buckets = {
        "<50KB": 0,
        "50-100KB": 0,
        "100-500KB": 0,
        "500KB-1MB": 0,
        ">1MB": 0,
    }

    for size_kb in size_kb_values:
        if size_kb < 50:
            size_buckets["<50KB"] += 1
        elif size_kb < 100:
            size_buckets["50-100KB"] += 1
        elif size_kb < 500:
            size_buckets["100-500KB"] += 1
        elif size_kb < 1024:
            size_buckets["500KB-1MB"] += 1
        else:
            size_buckets[">1MB"] += 1

    print(f"\\n=== SIZE DISTRIBUTION ===")
    for bucket, count in size_buckets.items():
        print(f"{bucket}: {count} DAGs")

    # Save results to XCom for downstream tasks
    return {
        "total_dags": total_dags,
        "avg_size_kb": avg_size_kb,
        "max_size_mb": max_size_mb,
        "min_size_kb": min_size_kb,
        "compression_stats": {
            "compressed_count": compressed_count,
            "uncompressed_count": uncompressed_count,
            "compressed_avg_kb": (
                sum(compressed_sizes) / len(compressed_sizes) if compressed_sizes else 0
            ),
            "uncompressed_avg_kb": (
                sum(uncompressed_sizes) / len(uncompressed_sizes)
                if uncompressed_sizes
                else 0
            ),
        },
        "size_distribution": size_buckets,
        "top_10_largest": top_10_records,
    }


@task()
def analyze_dag_performance(**context):
    """
    Analyze DAG performance metrics
    """
    # Get connection info from previous task
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    query = f"""
    SELECT 
        dag_id,
        COUNT(*) as total_runs,
        SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) as successful_runs,
        SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed_runs,
        ROUND(
            (SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 
            2
        ) as success_rate_percent,
        AVG(
            CASE 
                WHEN end_date IS NOT NULL AND start_date IS NOT NULL 
                THEN EXTRACT(EPOCH FROM (end_date - start_date))
                ELSE NULL 
            END
        ) as avg_runtime_seconds,
        MAX(
            CASE 
                WHEN end_date IS NOT NULL AND start_date IS NOT NULL 
                THEN EXTRACT(EPOCH FROM (end_date - start_date))
                ELSE NULL 
            END
        ) as max_runtime_seconds
    FROM dag_run
    WHERE execution_date >= CURRENT_DATE - INTERVAL '{ANALYSIS_DAYS} days'
    GROUP BY dag_id
    ORDER BY total_runs DESC
    """

    records = postgres_hook.get_records(query)

    if not records:
        print("No DAG run data found!")
        return {"active_dags": 0, "message": "No performance data available"}

    active_dags = len(records)
    success_rates = [float(record[4]) for record in records if record[4] is not None]
    avg_success_rate = sum(success_rates) / len(success_rates) if success_rates else 0

    print(f"\\n=== DAG PERFORMANCE ANALYSIS ===")
    print(f"Analysis period: Last {ANALYSIS_DAYS} days")
    print(f"Active DAGs: {active_dags}")

    # Performance summary
    print(f"\\n=== PERFORMANCE SUMMARY ===")
    print(
        f"{'DAG ID':<30} {'Total Runs':<12} {'Success Rate':<12} {'Avg Runtime (s)':<15}"
    )
    print("-" * 75)

    performance_summary = []
    for i, record in enumerate(records[:10]):
        dag_id = record[0]
        total_runs = record[1]
        success_rate = record[4] if record[4] is not None else 0
        avg_runtime = record[5] if record[5] is not None else 0

        print(
            f"{dag_id:<30} {total_runs:<12} {success_rate:<12.1f}% {avg_runtime:<15.1f}"
        )

        performance_summary.append(
            {
                "dag_id": dag_id,
                "total_runs": int(total_runs),
                "success_rate_percent": float(success_rate),
                "avg_runtime_seconds": float(avg_runtime),
            }
        )

    # Success rate distribution
    success_buckets = {"<50%": 0, "50-80%": 0, "80-95%": 0, "95-100%": 0}

    for rate in success_rates:
        if rate < 50:
            success_buckets["<50%"] += 1
        elif rate < 80:
            success_buckets["50-80%"] += 1
        elif rate < 95:
            success_buckets["80-95%"] += 1
        else:
            success_buckets["95-100%"] += 1

    print(f"\\n=== SUCCESS RATE DISTRIBUTION ===")
    for bucket, count in success_buckets.items():
        print(f"{bucket}: {count} DAGs")

    return {
        "active_dags": active_dags,
        "avg_success_rate": avg_success_rate,
        "performance_summary": performance_summary,
        "success_distribution": success_buckets,
    }


@task()
def analyze_task_failures(**context):
    """
    Analyze task failure patterns
    """
    # Get connection info from previous task
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    # First, check the schema of task_fail table
    schema_query = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns 
    WHERE table_name = 'task_fail'
    ORDER BY ordinal_position
    """

    try:
        schema_records = postgres_hook.get_records(schema_query)
        print(f"\\n=== TASK_FAIL TABLE SCHEMA ===")
        for record in schema_records:
            print(
                f"{record[0]}: {record[1]} ({'NULL' if record[2] == 'YES' else 'NOT NULL'})"
            )
    except Exception as e:
        print(f"Could not retrieve task_fail schema: {e}")

    # Simplified query without traceback (since column doesn't exist)
    query = f"""
    SELECT 
        dag_id,
        task_id,
        COUNT(*) as failure_count,
        MAX(start_date) as last_failure_date,
        MIN(start_date) as first_failure_date,
        COUNT(DISTINCT DATE(start_date)) as failure_days
    FROM task_fail tf
    WHERE start_date >= CURRENT_DATE - INTERVAL '{ANALYSIS_DAYS} days'
    GROUP BY dag_id, task_id
    ORDER BY failure_count DESC
    LIMIT 20
    """

    records = postgres_hook.get_records(query)

    print(f"\\n=== TASK FAILURE ANALYSIS ===")
    print(f"Analysis period: Last {ANALYSIS_DAYS} days")
    print(f"Failed task types: {len(records)}")

    if records:
        print(f"\\n=== TOP FAILING TASKS ===")
        print(
            f"{'DAG ID':<25} {'Task ID':<25} {'Failures':<10} {'Days':<6} {'Last Failure':<20}"
        )
        print("-" * 90)

        top_failures = []
        dag_failure_counts = {}

        for record in records:
            dag_id = record[0]
            task_id = record[1]
            failure_count = record[2]
            last_failure = record[3]
            first_failure = record[4]
            failure_days = record[5]

            print(
                f"{dag_id:<25} {task_id:<25} {failure_count:<10} {failure_days:<6} {str(last_failure):<20}"
            )

            top_failures.append(
                {
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "failure_count": int(failure_count),
                    "failure_days": int(failure_days),
                    "last_failure_date": str(last_failure),
                }
            )

            # Aggregate failures by DAG
            if dag_id in dag_failure_counts:
                dag_failure_counts[dag_id] += failure_count
            else:
                dag_failure_counts[dag_id] = failure_count

        # Sort DAG failures by count
        sorted_dag_failures = sorted(
            dag_failure_counts.items(), key=lambda x: x[1], reverse=True
        )

        print(f"\\n=== FAILURES BY DAG ===")
        print(f"{'DAG ID':<30} {'Total Failures':<15}")
        print("-" * 50)
        for dag_id, total_failures in sorted_dag_failures[:10]:
            print(f"{dag_id:<30} {total_failures:<15}")

        return {
            "total_failing_tasks": len(records),
            "top_failures": top_failures[:10],
            "dag_failure_summary": dict(sorted_dag_failures),
        }
    else:
        print("No task failures found in the analysis period!")
        return {"total_failing_tasks": 0, "top_failures": [], "dag_failure_summary": {}}


@task()
def create_analysis_summary(**context):
    """
    Create a comprehensive analysis summary
    """
    # Get results from previous tasks
    dag_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_serialized_dags"
    )
    performance_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_dag_performance"
    )
    failure_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_task_failures"
    )

    summary = {
        "analysis_date": context["ds"],
        "environment": ENV_NAME,
        "analysis_period_days": ANALYSIS_DAYS,
        "serialized_dag_metrics": {
            "total_dags": dag_analysis["total_dags"],
            "avg_size_kb": dag_analysis["avg_size_kb"],
            "max_size_mb": dag_analysis["max_size_mb"],
            "largest_dags": dag_analysis["top_10_largest"][:5],  # Top 5
        },
        "performance_metrics": {
            "active_dags": performance_analysis["active_dags"],
            "avg_success_rate": performance_analysis["avg_success_rate"],
            "top_performers": performance_analysis["performance_summary"][:5],
        },
        "failure_metrics": {
            "failing_task_types": failure_analysis["total_failing_tasks"],
            "top_failures": failure_analysis["top_failures"][:5],
        },
        "recommendations": [],
    }

    # Generate recommendations based on analysis
    if dag_analysis["max_size_mb"] > 5:
        summary["recommendations"].append(
            f"Consider optimizing large DAGs - largest is {dag_analysis['max_size_mb']:.1f}MB"
        )

    if performance_analysis["avg_success_rate"] < 95:
        summary["recommendations"].append(
            f"Overall success rate is {performance_analysis['avg_success_rate']:.1f}% - investigate failing DAGs"
        )

    if failure_analysis["total_failing_tasks"] > 10:
        summary["recommendations"].append(
            f"{failure_analysis['total_failing_tasks']} task types are failing - review error patterns"
        )

    if not summary["recommendations"]:
        summary["recommendations"].append(
            "System appears healthy - continue monitoring"
        )

    print(f"\\n=== COMPREHENSIVE ANALYSIS SUMMARY ===")
    print(f"Environment: {summary['environment']}")
    print(f"Analysis Date: {summary['analysis_date']}")
    print(f"Period: {summary['analysis_period_days']} days")
    print(f"\\nKey Metrics:")
    print(f"- Total DAGs: {summary['serialized_dag_metrics']['total_dags']}")
    print(
        f"- Average DAG Size: {summary['serialized_dag_metrics']['avg_size_kb']:.1f} KB"
    )
    print(f"- Active DAGs: {summary['performance_metrics']['active_dags']}")
    print(
        f"- Average Success Rate: {summary['performance_metrics']['avg_success_rate']:.1f}%"
    )
    print(f"- Failing Task Types: {summary['failure_metrics']['failing_task_types']}")
    print(f"\\nRecommendations:")
    for rec in summary["recommendations"]:
        print(f"- {rec}")

    return summary


# Define the DAG
@dag(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Direct PostgreSQL analysis of MWAA metadata with focus on serialized DAGs",
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=["postgresql", "mwaa", "metadata", "analysis", "direct"],
)
def postgres_mwaa_analysis_dag():

    # Task 1: Create PostgreSQL connection
    conn_setup = create_postgres_connection()

    # Task 2: Analyze serialized DAGs (main focus)
    dag_analysis = analyze_serialized_dags()

    # Task 3: Analyze DAG performance
    performance_analysis = analyze_dag_performance()

    # Task 4: Analyze task failures
    failure_analysis = analyze_task_failures()

    # Task 5: Create comprehensive summary
    summary = create_analysis_summary()

    # Set up dependencies
    conn_setup >> [dag_analysis, performance_analysis, failure_analysis] >> summary


# Instantiate the DAG
postgres_mwaa_analysis_dag_instance = postgres_mwaa_analysis_dag()
