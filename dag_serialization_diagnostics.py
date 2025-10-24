import os
import json
import re
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

## DAG Serialization Diagnostics Settings

# Analysis time window (days)
ANALYSIS_DAYS = Variable.get("analysis_days", default_var=7)

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

    env_name_lower = ENV_NAME.lower() if ENV_NAME else "mwaa"
    conn_id = f"{env_name_lower}_postgres_diag"

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
            description=f"MWAA PostgreSQL diagnostics connection for {ENV_NAME}",
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
def analyze_serialization_issues(**context):
    """
    Deep analysis of DAG serialization issues
    """
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("DAG SERIALIZATION ISSUE DIAGNOSTICS")
    print("=" * 80)

    # 1. Identify problematic DAGs by size
    large_dags_query = """
    SELECT 
        dag_id,
        fileloc,
        LENGTH(data::text) as size_bytes,
        ROUND(LENGTH(data::text) / 1024.0, 2) as size_kb,
        ROUND(LENGTH(data::text) / 1048576.0, 2) as size_mb,
        last_updated,
        dag_hash,
        -- Count JSON nesting levels (indicator of complexity)
        (LENGTH(data::text) - LENGTH(REPLACE(data::text, '{', ''))) as open_braces,
        (LENGTH(data::text) - LENGTH(REPLACE(data::text, '}', ''))) as close_braces,
        -- Count array elements
        (LENGTH(data::text) - LENGTH(REPLACE(data::text, '[', ''))) as open_brackets,
        (LENGTH(data::text) - LENGTH(REPLACE(data::text, ']', ''))) as close_brackets
    FROM serialized_dag
    WHERE data IS NOT NULL
    ORDER BY size_bytes DESC
    """

    large_dags = postgres_hook.get_records(large_dags_query)

    print("\n1. LARGE DAG ANALYSIS (Potential Serialization Issues)")
    print("-" * 60)
    print(
        f"{'DAG ID':<30} {'Size (MB)':<10} {'Braces':<8} {'Brackets':<10} {'File':<30}"
    )
    print("-" * 90)

    suspicious_dags = []
    for record in large_dags[:15]:  # Top 15 largest
        dag_id = record[0]
        size_mb = float(record[4])
        open_braces = record[7]
        close_braces = record[8]
        open_brackets = record[9]
        close_brackets = record[10]
        fileloc = record[1]

        # Detect potential issues
        is_suspicious = False
        issues = []

        if size_mb > 5:  # Over 5MB is suspicious
            issues.append("LARGE_SIZE")
            is_suspicious = True

        if open_braces != close_braces:
            issues.append("UNBALANCED_BRACES")
            is_suspicious = True

        if open_brackets != close_brackets:
            issues.append("UNBALANCED_BRACKETS")
            is_suspicious = True

        if open_braces > 10000:  # Excessive nesting
            issues.append("DEEP_NESTING")
            is_suspicious = True

        status = "⚠️" if is_suspicious else "✅"
        print(
            f"{dag_id:<30} {size_mb:<10.2f} {open_braces:<8} {open_brackets:<10} {fileloc[-30:]:<30} {status}"
        )

        if is_suspicious:
            suspicious_dags.append(
                {
                    "dag_id": dag_id,
                    "size_mb": size_mb,
                    "issues": issues,
                    "fileloc": fileloc,
                }
            )

    # 2. Analyze serialization patterns for problematic DAGs
    if suspicious_dags:
        print("\n2. DETAILED ANALYSIS OF SUSPICIOUS DAGs")
        print("-" * 60)

        for dag_info in suspicious_dags[:5]:  # Top 5 most suspicious
            dag_id = dag_info["dag_id"]
            print(f"\nAnalyzing: {dag_id}")
            print(f"Issues: {', '.join(dag_info['issues'])}")

            # Get detailed serialization data
            detail_query = f"""
            SELECT 
                data::text,
                LENGTH(data::text) as total_size,
                dag_hash
            FROM serialized_dag 
            WHERE dag_id = '{dag_id}'
            """

            detail_records = postgres_hook.get_records(detail_query)
            if detail_records:
                serialized_data = detail_records[0][0]

                # Analyze serialization content
                analysis = analyze_serialization_content(serialized_data, dag_id)

                for key, value in analysis.items():
                    print(f"  {key}: {value}")

    # 3. Check for duplicate key violations in logs (if accessible)
    print("\n3. SERIALIZATION CONFLICT ANALYSIS")
    print("-" * 60)

    # Check for DAGs with frequent updates (indicator of serialization loops)
    frequent_updates_query = f"""
    SELECT 
        dag_id,
        COUNT(*) as update_frequency,
        MIN(last_updated) as first_seen,
        MAX(last_updated) as last_seen,
        COUNT(DISTINCT dag_hash) as hash_variations
    FROM (
        SELECT 
            dag_id,
            last_updated,
            dag_hash,
            ROW_NUMBER() OVER (PARTITION BY dag_id ORDER BY last_updated DESC) as rn
        FROM serialized_dag
        WHERE last_updated >= CURRENT_TIMESTAMP - INTERVAL '{ANALYSIS_DAYS} days'
    ) recent_updates
    WHERE rn <= 100  -- Look at recent updates
    GROUP BY dag_id
    HAVING COUNT(DISTINCT dag_hash) > 5  -- Multiple hash changes indicate instability
    ORDER BY update_frequency DESC, hash_variations DESC
    """

    try:
        frequent_updates = postgres_hook.get_records(frequent_updates_query)

        if frequent_updates:
            print("DAGs with frequent serialization changes (potential loops):")
            print(f"{'DAG ID':<30} {'Updates':<8} {'Hash Vars':<10} {'Time Range':<25}")
            print("-" * 75)

            for record in frequent_updates:
                dag_id = record[0]
                updates = record[1]
                hash_vars = record[4]
                time_range = f"{record[2]} to {record[3]}"

                print(
                    f"{dag_id:<30} {updates:<8} {hash_vars:<10} {str(record[2])[:19]:<25}"
                )
        else:
            print("No DAGs with excessive serialization changes detected.")

    except Exception as e:
        print(f"Could not analyze update frequency: {e}")

    # 4. Summary statistics
    print("\n4. ANALYSIS SUMMARY")
    print("-" * 60)
    
    total_suspicious = len(suspicious_dags)
    total_analyzed = len(large_dags)
    
    print(f"Total DAGs analyzed: {total_analyzed}")
    print(f"Suspicious DAGs found: {total_suspicious}")
    
    if total_suspicious > 0:
        avg_size = sum(dag['size_mb'] for dag in suspicious_dags) / total_suspicious
        max_size = max(dag['size_mb'] for dag in suspicious_dags)
        print(f"Average suspicious DAG size: {avg_size:.2f} MB")
        print(f"Largest suspicious DAG size: {max_size:.2f} MB")
        
        # Issue type breakdown
        issue_counts = {}
        for dag in suspicious_dags:
            for issue in dag['issues']:
                issue_counts[issue] = issue_counts.get(issue, 0) + 1
        
        print("\nIssue type breakdown:")
        for issue, count in sorted(issue_counts.items()):
            print(f"  {issue}: {count} DAGs")
    else:
        print("No suspicious DAGs detected.")

    # Create detailed analysis report
    detailed_report_lines = []
    detailed_report_lines.append("=" * 80)
    detailed_report_lines.append("DETAILED DAG SERIALIZATION ANALYSIS")
    detailed_report_lines.append("=" * 80)
    detailed_report_lines.append(
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    detailed_report_lines.append(f"Environment: {ENV_NAME}")
    detailed_report_lines.append(f"Total DAGs Analyzed: {len(large_dags)}")
    detailed_report_lines.append("")

    detailed_report_lines.append("ALL DAGs BY SIZE")
    detailed_report_lines.append("-" * 40)
    detailed_report_lines.append(
        f"{'DAG ID':<35} {'Size (MB)':<10} {'Tasks':<8} {'Risk':<15} {'File':<30}"
    )
    detailed_report_lines.append("-" * 100)

    for record in large_dags:
        dag_id = record[0]
        size_mb = float(record[4])
        task_count = record[7] if record[7] else 0
        fileloc = record[1][-30:] if record[1] else "Unknown"

        # Determine risk level
        if size_mb > 5:
            risk = "CRITICAL"
        elif size_mb > 1:
            risk = "HIGH"
        elif size_mb > 0.5:
            risk = "MEDIUM"
        else:
            risk = "LOW"

        detailed_report_lines.append(
            f"{dag_id:<35} {size_mb:<10.2f} {task_count:<8} {risk:<15} {fileloc:<30}"
        )

    if suspicious_dags:
        detailed_report_lines.append("")
        detailed_report_lines.append("SUSPICIOUS DAGs DETAILED ANALYSIS")
        detailed_report_lines.append("-" * 40)

        for dag_info in suspicious_dags:
            detailed_report_lines.append(f"\nDAG: {dag_info['dag_id']}")
            detailed_report_lines.append(f"  File: {dag_info['fileloc']}")
            detailed_report_lines.append(f"  Size: {dag_info['size_mb']:.2f} MB")
            detailed_report_lines.append(f"  Issues: {', '.join(dag_info['issues'])}")

    detailed_report_lines.append("")
    detailed_report_lines.append("ANALYSIS SUMMARY")
    detailed_report_lines.append("-" * 40)
    detailed_report_lines.append(f"Total DAGs analyzed: {len(large_dags)}")
    detailed_report_lines.append(f"Suspicious DAGs found: {len(suspicious_dags)}")
    
    if suspicious_dags:
        avg_size = sum(dag['size_mb'] for dag in suspicious_dags) / len(suspicious_dags)
        max_size = max(dag['size_mb'] for dag in suspicious_dags)
        detailed_report_lines.append(f"Average suspicious DAG size: {avg_size:.2f} MB")
        detailed_report_lines.append(f"Largest suspicious DAG size: {max_size:.2f} MB")
        
        # Issue type breakdown
        issue_counts = {}
        for dag in suspicious_dags:
            for issue in dag['issues']:
                issue_counts[issue] = issue_counts.get(issue, 0) + 1
        
        detailed_report_lines.append("")
        detailed_report_lines.append("Issue type breakdown:")
        for issue, count in sorted(issue_counts.items()):
            detailed_report_lines.append(f"  {issue}: {count} DAGs")

    detailed_report = "\n".join(detailed_report_lines)

    # Save detailed analysis to S3
    save_diagnostics_report_to_s3(detailed_report, context, "detailed_analysis")

    return {
        "suspicious_dags": suspicious_dags,
        "total_analyzed": len(large_dags),
        "detailed_report": detailed_report,
    }


def analyze_serialization_content(serialized_data, dag_id):
    """
    Analyze the content of serialized DAG data for common issues
    """
    analysis = {}

    try:
        # Parse as JSON to validate structure
        data = json.loads(serialized_data)
        analysis["json_valid"] = True

        # Check for common problematic patterns
        data_str = json.dumps(data)

        # Circular reference indicators
        if '"_dag"' in data_str and data_str.count('"_dag"') > 10:
            dag_count = data_str.count('"_dag"')
            analysis["potential_circular_refs"] = f"Found {dag_count} _dag references"

        # Large embedded objects
        if '"pickle"' in data_str or '"__reduce__"' in data_str:
            analysis["pickle_detected"] = "Pickle serialization detected (problematic)"

        # Excessive task references
        task_count = data_str.count('"task_id"')
        if task_count > 1000:
            analysis["excessive_tasks"] = (
                f"{task_count} task references (may indicate duplication)"
            )

        # Deep nesting
        max_depth = get_json_depth(data)
        if max_depth > 20:
            analysis["deep_nesting"] = f"JSON depth: {max_depth} levels (excessive)"

        # Large string values
        large_strings = find_large_strings(data_str)
        if large_strings:
            analysis["large_strings"] = f"Found {len(large_strings)} strings > 10KB"

        # Repetitive patterns
        repetitive = find_repetitive_patterns(data_str)
        if repetitive:
            analysis["repetitive_patterns"] = (
                f"Detected repetitive content: {repetitive}"
            )

    except json.JSONDecodeError as e:
        analysis["json_valid"] = False
        analysis["json_error"] = str(e)
    except Exception as e:
        analysis["analysis_error"] = str(e)

    return analysis


def get_json_depth(obj, depth=0):
    """Calculate maximum depth of JSON object"""
    if isinstance(obj, dict):
        return max([get_json_depth(v, depth + 1) for v in obj.values()], default=depth)
    elif isinstance(obj, list):
        return max([get_json_depth(item, depth + 1) for item in obj], default=depth)
    else:
        return depth


def find_large_strings(data_str):
    """Find strings larger than 10KB in serialized data"""
    large_strings = []
    # Simple regex to find quoted strings
    strings = re.findall(r'"([^"]{10240,})"', data_str)  # 10KB+
    return len(strings)


def find_repetitive_patterns(data_str):
    """Find repetitive patterns that might indicate serialization loops"""
    # Look for repeated substrings
    sample_size = min(1000, len(data_str) // 10)
    if sample_size < 100:
        return None

    sample = data_str[:sample_size]

    # Count occurrences of the sample in the full string
    occurrences = data_str.count(sample)

    if occurrences > 5:  # Same pattern repeated more than 5 times
        return f"Pattern repeated {occurrences} times"

    return None


def save_diagnostics_report_to_s3(report_content, context, report_type):
    """
    Save diagnostics report to S3 in text format
    """
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        # Get S3 bucket from Variable
        s3_bucket = Variable.get(
            "results_s3_bucket", "aws-athena-query-results-us-east-1-515232103838"
        )

        # Create S3 hook
        s3_hook = S3Hook(aws_conn_id="aws_default")

        # Generate S3 key
        execution_date = context["ds"]
        timestamp = context["ts"].replace(":", "-").replace("+", "_")

        s3_key = (
            f"mwaa_analysis/{ENV_NAME}/{execution_date}/{report_type}_{timestamp}.txt"
        )

        # Upload report to S3
        s3_hook.load_string(
            string_data=report_content, key=s3_key, bucket_name=s3_bucket, replace=True
        )

        s3_location = f"s3://{s3_bucket}/{s3_key}"
        print(f"\n📄 Diagnostics report saved to S3: {s3_location}")

        return {
            "status": "success",
            "location": s3_location,
            "bucket": s3_bucket,
            "key": s3_key,
        }

    except Exception as e:
        print(f"❌ Failed to save diagnostics report to S3: {e}")
        return {"status": "failed", "error": str(e)}


@task()
def generate_serialization_report(**context):
    """
    Generate comprehensive serialization diagnostics report
    """
    analysis_result = context["task_instance"].xcom_pull(
        task_ids="analyze_serialization_issues"
    )

    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("DAG SERIALIZATION DIAGNOSTICS REPORT")
    report_lines.append("=" * 80)
    report_lines.append(
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    report_lines.append(f"Environment: {ENV_NAME}")
    report_lines.append("")

    if analysis_result["suspicious_dags"]:
        report_lines.append("🚨 CRITICAL FINDINGS")
        report_lines.append("-" * 40)

        for dag_info in analysis_result["suspicious_dags"]:
            report_lines.append(f"DAG: {dag_info['dag_id']}")
            report_lines.append(f"  Size: {dag_info['size_mb']:.2f} MB")
            report_lines.append(f"  Issues: {', '.join(dag_info['issues'])}")
            report_lines.append(f"  File: {dag_info['fileloc']}")
            report_lines.append("")

        # Issue breakdown
        issue_counts = {}
        for dag_info in analysis_result["suspicious_dags"]:
            for issue in dag_info['issues']:
                issue_counts[issue] = issue_counts.get(issue, 0) + 1
        
        report_lines.append("📊 ISSUE BREAKDOWN")
        report_lines.append("-" * 40)
        for issue, count in sorted(issue_counts.items()):
            report_lines.append(f"{issue}: {count} DAGs")
        
        avg_size = sum(dag['size_mb'] for dag in analysis_result["suspicious_dags"]) / len(analysis_result["suspicious_dags"])
        max_size = max(dag['size_mb'] for dag in analysis_result["suspicious_dags"])
        
        report_lines.append("")
        report_lines.append("📈 SIZE STATISTICS")
        report_lines.append("-" * 40)
        report_lines.append(f"Average suspicious DAG size: {avg_size:.2f} MB")
        report_lines.append(f"Largest suspicious DAG size: {max_size:.2f} MB")
        report_lines.append(f"Total DAGs analyzed: {analysis_result['total_analyzed']}")
        report_lines.append(f"Suspicious DAGs found: {len(analysis_result['suspicious_dags'])}")

    else:
        report_lines.append("✅ No critical serialization issues detected")
        report_lines.append("All DAGs appear to have reasonable serialization sizes")
        report_lines.append(f"Total DAGs analyzed: {analysis_result['total_analyzed']}")
        
        # Show size distribution for all DAGs
        report_lines.append("")
        report_lines.append("📊 SIZE DISTRIBUTION")
        report_lines.append("-" * 40)
        report_lines.append("All DAGs have normal serialization sizes")

    report_lines.append("")
    report_lines.append("=" * 80)

    report = "\n".join(report_lines)
    print("\n" + report)

    # Save report to S3
    s3_result = save_diagnostics_report_to_s3(
        report, context, "serialization_diagnostics"
    )

    return {
        "report": report,
        "critical_issues": len(analysis_result["suspicious_dags"]),
        "s3_location": s3_result,
    }


@task()
def cleanup_postgres_connection(**context):
    """Clean up the diagnostic connection"""
    from airflow.models import Connection
    from airflow import settings

    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    session = settings.Session()
    try:
        conn_to_delete = (
            session.query(Connection).filter(Connection.conn_id == conn_id).first()
        )
        if conn_to_delete:
            session.delete(conn_to_delete)
            session.commit()
            print(f"Deleted diagnostic connection: {conn_id}")
        return {"status": "deleted"}
    except Exception as e:
        session.rollback()
        print(f"Failed to delete connection: {e}")
        return {"status": "failed"}
    finally:
        session.close()


# Define the DAG
@dag(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Diagnose DAG serialization issues and exponential parsing times",
    schedule_interval=None,  # Manual trigger for diagnostics
    catchup=False,
    tags=["diagnostics", "serialization", "debugging", "performance"],
)
def dag_serialization_diagnostics_dag():

    # Task 1: Create connection
    conn_setup = create_postgres_connection()

    # Task 2: Analyze serialization issues
    analysis = analyze_serialization_issues()

    # Task 3: Generate comprehensive report
    report = generate_serialization_report()

    # Task 4: Cleanup
    cleanup = cleanup_postgres_connection()

    # Set dependencies
    conn_setup >> analysis >> report >> cleanup


# Instantiate the DAG
dag_serialization_diagnostics_dag_instance = dag_serialization_diagnostics_dag()
