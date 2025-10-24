"""
DAG Diagnostics System for MWAA
Combines serialization analysis, performance monitoring, and configuration tracking
Compatible with Apache Airflow v2.7+ and v3.10+
"""

import os
import json
import re
import hashlib
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

# Core analysis parameters
ANALYSIS_DAYS = Variable.get("dag_diagnostics_analysis_days", default_var=7)
ENV_NAME = Variable.get(
    "dag_diagnostics_env_name", default_var=os.getenv("AIRFLOW_ENV_NAME", "")
)
S3_BUCKET = Variable.get(
    "dag_diagnostics_s3_bucket",
    default_var="",
)

# Thresholds for analysis (configurable)
LARGE_DAG_SIZE_MB = float(
    Variable.get("dag_diagnostics_large_dag_threshold_mb", default_var=5.0)
)
CRITICAL_DAG_SIZE_MB = float(
    Variable.get("dag_diagnostics_critical_dag_threshold_mb", default_var=10.0)
)
MIN_SUCCESS_RATE = float(
    Variable.get("dag_diagnostics_min_success_rate", default_var=95.0)
)
MAX_NESTING_DEPTH = int(
    Variable.get("dag_diagnostics_max_nesting_depth", default_var=20)
)

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
# EXTENSIBLE DIAGNOSTIC QUERIES REGISTRY
# =============================================================================


class DiagnosticQueries:
    """Registry for extensible diagnostic queries"""

    @staticmethod
    def get_serialized_dag_analysis_query(analysis_days: int) -> str:
        """Enhanced serialized DAG analysis query"""
        return f"""
        SELECT 
            dag_id,
            fileloc,
            CASE 
                WHEN data_compressed IS NOT NULL AND LENGTH(data_compressed) > 0 THEN 'Compressed'
                ELSE 'Uncompressed'
            END as compression_status,
            -- More accurate size calculation using JSON representation
            round(octet_length(data::json::text), 2) as serialized_size_bytes,
            round(octet_length(data::json::text) / 1024.0, 2) as serialized_size_kb,
            round(octet_length(data::json::text) / 1024.0 / 1024.0, 2) as serialized_size_mb,
            -- Alternative: use pg_column_size for actual storage size
            pg_column_size(data) as storage_size_bytes,
            round(pg_column_size(data) / 1024.0, 2) as storage_size_kb,
            round(pg_column_size(data) / 1024.0 / 1024.0, 2) as storage_size_mb,
            last_updated,
            EXTRACT(DAY FROM (CURRENT_TIMESTAMP - last_updated)) as days_since_update,
            dag_hash,
            processor_subdir,
            -- Enhanced task count estimation
            CASE 
                WHEN data::text LIKE '%"task_id"%' THEN 
                    (LENGTH(data::text) - LENGTH(REPLACE(data::text, '"task_id"', ''))) / LENGTH('"task_id"')
                ELSE 0
            END as estimated_task_count,
            -- Operator type detection
            CASE 
                WHEN data::text LIKE '%BashOperator%' THEN 'BashOperator'
                WHEN data::text LIKE '%PythonOperator%' THEN 'PythonOperator'
                WHEN data::text LIKE '%S3%' THEN 'S3Operations'
                WHEN data::text LIKE '%Glue%' THEN 'GlueOperator'
                WHEN data::text LIKE '%Kubernetes%' THEN 'KubernetesOperator'
                WHEN data::text LIKE '%Docker%' THEN 'DockerOperator'
                ELSE 'Other'
            END as primary_operator_type,
            -- Complexity indicators
            CASE 
                WHEN data::text LIKE '%depends_on_past%' THEN 'Has Dependencies'
                WHEN data::text LIKE '%retries%' THEN 'Has Retries'
                WHEN data::text LIKE '%pool%' THEN 'Uses Pools'
                WHEN data::text LIKE '%sla%' THEN 'Has SLA'
                ELSE 'Standard'
            END as complexity_indicators,
            -- Basic structure info
            LENGTH(data::text) as total_length
        FROM serialized_dag
        WHERE data IS NOT NULL
        ORDER BY LENGTH(data::text) DESC
        """

    @staticmethod
    def get_performance_analysis_query(analysis_days: int) -> str:
        """Enhanced DAG performance analysis query"""
        return f"""
        SELECT 
            dag_id,
            COUNT(*) as total_runs,
            SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) as successful_runs,
            SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed_runs,
            SUM(CASE WHEN state = 'running' THEN 1 ELSE 0 END) as running_runs,
            SUM(CASE WHEN state = 'queued' THEN 1 ELSE 0 END) as queued_runs,
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
            ) as max_runtime_seconds,
            MIN(
                CASE 
                    WHEN end_date IS NOT NULL AND start_date IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (end_date - start_date))
                    ELSE NULL 
                END
            ) as min_runtime_seconds,
            COUNT(DISTINCT DATE(execution_date)) as active_days
        FROM dag_run
        WHERE execution_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
        GROUP BY dag_id
        ORDER BY total_runs DESC
        """

    @staticmethod
    def get_task_failure_analysis_query(analysis_days: int) -> str:
        """Enhanced task failure analysis query"""
        return f"""
        SELECT 
            tf.dag_id,
            tf.task_id,
            COUNT(*) as failure_count,
            MAX(tf.start_date) as last_failure_date,
            MIN(tf.start_date) as first_failure_date,
            COUNT(DISTINCT DATE(tf.start_date)) as failure_days,
            AVG(
                CASE 
                    WHEN tf.end_date IS NOT NULL AND tf.start_date IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (tf.end_date - tf.start_date))
                    ELSE NULL 
                END
            ) as avg_failure_duration_seconds
        FROM task_fail tf
        WHERE tf.start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
        GROUP BY tf.dag_id, tf.task_id
        ORDER BY failure_count DESC
        LIMIT 50
        """

    @staticmethod
    def get_serialization_instability_query(analysis_days: int) -> str:
        """Query to detect DAGs with frequent serialization changes"""
        return f"""
        SELECT 
            dag_id,
            COUNT(*) as update_frequency,
            MIN(last_updated) as first_seen,
            MAX(last_updated) as last_seen,
            COUNT(DISTINCT dag_hash) as hash_variations,
            AVG(octet_length(data::json::text)) as avg_size_bytes,
            MAX(octet_length(data::json::text)) as max_size_bytes,
            MIN(octet_length(data::json::text)) as min_size_bytes
        FROM serialized_dag
        WHERE last_updated >= CURRENT_TIMESTAMP - INTERVAL '{analysis_days} days'
        GROUP BY dag_id
        HAVING COUNT(DISTINCT dag_hash) > 3
        ORDER BY hash_variations DESC, update_frequency DESC
        """


# =============================================================================
# CONFIGURATION ANALYSIS HELPERS
# =============================================================================


def analyze_config_setting(
    section: str, setting: str, value: str, setting_info: Dict[str, Any]
) -> str:
    """Analyze a configuration setting and return status"""

    if value == "NOT_SET":
        return "🔶 Default"

    # Specific analysis for critical settings
    if section == "core":
        if setting == "executor" and "celery" not in value.lower():
            return "⚠️ Non-Celery"
        elif setting == "parallelism":
            try:
                parallelism = int(value)
                if parallelism > 100:
                    return "⚠️ High"
                elif parallelism < 10:
                    return "⚠️ Low"
                else:
                    return "✅ Good"
            except ValueError:
                return "❌ Invalid"
        elif setting == "dag_file_processor_timeout":
            try:
                timeout = int(value)
                if timeout < 30:
                    return "⚠️ Too Low"
                elif timeout > 300:
                    return "⚠️ Too High"
                else:
                    return "✅ Good"
            except ValueError:
                return "❌ Invalid"
        elif setting in ["store_serialized_dags", "compress_serialized_dags"]:
            if value.lower() in ["true", "1", "yes"]:
                return "✅ Enabled"
            else:
                return "⚠️ Disabled"

    elif section == "scheduler":
        if setting == "dag_dir_list_interval":
            try:
                interval = int(value)
                if interval < 30:
                    return "⚠️ Too Frequent"
                elif interval > 300:
                    return "⚠️ Too Slow"
                else:
                    return "✅ Good"
            except ValueError:
                return "❌ Invalid"
        elif setting == "parsing_processes":
            try:
                processes = int(value)
                if processes < 1:
                    return "❌ Too Low"
                elif processes > 8:
                    return "⚠️ High"
                else:
                    return "✅ Good"
            except ValueError:
                return "❌ Invalid"

    elif section == "celery":
        if setting == "worker_concurrency":
            try:
                concurrency = int(value)
                if concurrency < 4:
                    return "⚠️ Low"
                elif concurrency > 32:
                    return "⚠️ High"
                else:
                    return "✅ Good"
            except ValueError:
                return "❌ Invalid"
        elif setting in ["broker_url", "result_backend"]:
            if "redis" in value.lower():
                return "✅ Redis"
            elif "sqs" in value.lower():
                return "✅ SQS"
            elif "rabbitmq" in value.lower():
                return "✅ RabbitMQ"
            else:
                return "🔶 Other"

    elif section == "database":
        if setting == "sql_alchemy_pool_size":
            try:
                pool_size = int(value)
                if pool_size < 5:
                    return "⚠️ Small"
                elif pool_size > 50:
                    return "⚠️ Large"
                else:
                    return "✅ Good"
            except ValueError:
                return "❌ Invalid"

    # Default status for configured values
    return "✅ Set"


def analyze_configuration_issues(config_analysis: Dict[str, Any]) -> List[str]:
    """Analyze configuration for potential stability issues"""
    issues = []

    try:
        # Check core settings
        core_config = config_analysis.get("core", {})

        # Check if serialized DAGs are enabled
        if core_config.get("store_serialized_dags", {}).get("value") not in [
            "True",
            "true",
            "1",
        ]:
            issues.append("Serialized DAGs not enabled - may impact performance")

        # Check parallelism settings
        parallelism = core_config.get("parallelism", {}).get("value", "32")
        max_active_tasks = core_config.get("max_active_tasks_per_dag", {}).get(
            "value", "16"
        )
        try:
            if int(parallelism) < int(max_active_tasks):
                issues.append(
                    f"Global parallelism ({parallelism}) < max_active_tasks_per_dag ({max_active_tasks})"
                )
        except (ValueError, TypeError):
            pass

        # Check scheduler settings
        scheduler_config = config_analysis.get("scheduler", {})

        # Check DAG parsing processes
        parsing_processes = scheduler_config.get("parsing_processes", {}).get(
            "value", "2"
        )
        try:
            if int(parsing_processes) < 2:
                issues.append("Low number of DAG parsing processes - may cause delays")
        except (ValueError, TypeError):
            pass

        # Check Celery settings
        celery_config = config_analysis.get("celery", {})

        # Check worker concurrency
        worker_concurrency = celery_config.get("worker_concurrency", {}).get(
            "value", "16"
        )
        try:
            if int(worker_concurrency) > int(parallelism):
                issues.append(
                    f"Celery worker_concurrency ({worker_concurrency}) > core parallelism ({parallelism})"
                )
        except (ValueError, TypeError):
            pass

        # Check database settings
        db_config = config_analysis.get("database", {})

        # Check connection pool size
        pool_size = db_config.get("sql_alchemy_pool_size", {}).get("value", "5")
        try:
            if int(pool_size) < 10:
                issues.append(
                    "Small database connection pool - may cause connection issues under load"
                )
        except (ValueError, TypeError):
            pass

    except Exception as e:
        issues.append(f"Error analyzing configuration: {str(e)}")

    return issues


# =============================================================================
# CORE DIAGNOSTIC FUNCTIONS
# =============================================================================


@task()
def create_postgres_connection():
    """Create PostgreSQL connection from MWAA environment variables"""
    from airflow.models import Connection
    from airflow import settings

    # Try both possible environment variable names for compatibility
    sql_alchemy_conn = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN") or os.getenv(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
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
    conn_id = f"{env_name_lower}_postgres_diagnostics"

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
def capture_airflow_configuration(**context):
    """Capture stability-critical Airflow configuration settings"""

    print("\n" + "=" * 80)
    print("AIRFLOW STABILITY CONFIGURATION ANALYSIS")
    print("=" * 80)

    # Define stability-critical configuration settings to capture
    stability_settings = {
        "core": [
            "executor",
            "parallelism",
            "max_active_tasks_per_dag",
            "max_active_runs_per_dag",
            "dagbag_import_timeout",
            "dag_file_processor_timeout",
            "killed_task_cleanup_time",
            "dag_discovery_safe_mode",
            "store_dag_code",
            "store_serialized_dags",
            "compress_serialized_dags",
            "min_serialized_dag_update_interval",
            "min_serialized_dag_fetch_interval",
        ],
        "scheduler": [
            "dag_dir_list_interval",
            "catchup_by_default",
            "max_dagruns_to_create_per_loop",
            "max_dagruns_per_loop_to_schedule",
            "pool_metrics_interval",
            "orphaned_tasks_check_interval",
            "child_process_log_directory",
            "parsing_processes",
            "file_parsing_sort_mode",
            "use_row_level_locking",
            "max_tis_per_query",
        ],
        "celery": [
            "worker_concurrency",
            "broker_url",
            "result_backend",
            "flower_host",
            "flower_port",
            "default_queue",
            "sync",
            "pool",
            "worker_precheck",
            "task_track_started",
            "task_publish_retry",
            "worker_enable_remote_control",
        ],
        "webserver": [
            "workers",
            "worker_timeout",
            "worker_refresh_batch_size",
            "reload_on_plugin_change",
        ],
        "database": [
            "sql_alchemy_pool_size",
            "sql_alchemy_max_overflow",
            "sql_alchemy_pool_recycle",
            "sql_alchemy_pool_pre_ping",
            "sql_alchemy_schema",
        ],
    }

    # Impact levels for display
    impact_levels = {
        "executor": "HIGH",
        "parallelism": "HIGH",
        "max_active_tasks_per_dag": "HIGH",
        "max_active_runs_per_dag": "HIGH",
        "dag_file_processor_timeout": "HIGH",
        "store_serialized_dags": "HIGH",
        "min_serialized_dag_update_interval": "HIGH",
        "dag_dir_list_interval": "HIGH",
        "parsing_processes": "HIGH",
        "worker_concurrency": "HIGH",
        "broker_url": "HIGH",
        "result_backend": "HIGH",
        "sql_alchemy_pool_size": "HIGH",
    }

    # Sensitive settings to mask
    sensitive_settings = ["broker_url", "result_backend"]

    # Capture configuration values
    config_data = {}

    print(f"{'Setting':<40} {'Value':<25} {'Impact':<8} {'Status':<15}")
    print("-" * 95)

    for section_name, settings in stability_settings.items():
        config_data[section_name] = {}

        for setting_name in settings:
            try:
                # Get the configuration value
                value = conf.get(section_name, setting_name, fallback="NOT_SET")

                # Determine impact level
                impact = impact_levels.get(setting_name, "MEDIUM")

                # Determine status
                if value == "NOT_SET":
                    status = "🔶 Default"
                elif setting_name in sensitive_settings:
                    status = "✅ Set"
                else:
                    status = analyze_config_setting(
                        section_name, setting_name, value, {"stability_impact": impact}
                    )

                # Store actual value (mask sensitive ones)
                if setting_name in sensitive_settings and value != "NOT_SET":
                    config_data[section_name][setting_name] = "***MASKED***"
                    display_value = "***MASKED***"
                else:
                    config_data[section_name][setting_name] = str(value)
                    display_value = str(value)
                    if len(display_value) > 22:
                        display_value = display_value[:19] + "..."

                # Display in table format
                full_name = f"{section_name}.{setting_name}"
                print(f"{full_name:<40} {display_value:<25} {impact:<8} {status:<15}")

            except Exception as e:
                config_data[section_name][setting_name] = f"ERROR: {str(e)}"
                print(
                    f"{section_name}.{setting_name:<35} ERROR: {str(e):<20} {impact_levels.get(setting_name, 'MEDIUM'):<8} ❌ Error"
                )

    # Add environment information
    try:
        import airflow

        import platform
        
        config_data["environment_info"] = {
            "airflow_version": airflow.__version__,
            "python_version": platform.python_version(),
            "executor": conf.get("core", "executor", fallback="Unknown"),
            "dags_folder": conf.get("core", "dags_folder", fallback="Unknown"),
        }

        print(f"\n📋 ENVIRONMENT INFO")
        print(f"Airflow Version: {airflow.__version__}")
        print(f"Python Version: {config_data['environment_info']['python_version']}")
        print(f"Executor: {config_data['environment_info']['executor']}")
        print(f"DAGs Folder: {config_data['environment_info']['dags_folder']}")

    except Exception as e:
        config_data["environment_info"] = {"error": str(e)}

    # Create configuration fingerprint for change detection
    config_json = json.dumps(config_data, sort_keys=True)
    config_hash = hashlib.md5(config_json.encode()).hexdigest()

    # Get runtime parameters for environment name
    dag_run = context.get("dag_run")
    params = context.get("params", {})

    if dag_run and dag_run.conf and "env_name" in dag_run.conf:
        env_name = dag_run.conf["env_name"]
    else:
        env_name = params.get("env_name", ENV_NAME)

    # Get previous configuration hash from Variable (if exists)
    previous_hash = Variable.get(
        f"dag_diagnostics_config_hash_{env_name}", default_var=None
    )
    config_changed = previous_hash is not None and previous_hash != config_hash

    # Update stored hash
    Variable.set(f"dag_diagnostics_config_hash_{env_name}", config_hash)

    print(f"\n🔍 CONFIGURATION CHANGE DETECTION")
    print(f"Configuration hash: {config_hash}")
    print(f"Previous hash: {previous_hash}")
    print(f"Configuration changed: {'YES' if config_changed else 'NO'}")

    return {
        "timestamp": datetime.now().isoformat(),
        "config_hash": config_hash,
        "previous_hash": previous_hash,
        "config_changed": config_changed,
        "configuration": config_data,
        "total_settings": sum(
            len(section)
            for section in config_data.values()
            if isinstance(section, dict)
        ),
    }


@task()
def analyze_serialized_dags(**context):
    """Serialized DAG analysis"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("SERIALIZED DAG ANALYSIS")
    print("=" * 80)

    # Execute main analysis query
    query = DiagnosticQueries.get_serialized_dag_analysis_query(ANALYSIS_DAYS)

    try:
        records = postgres_hook.get_records(query)
    except Exception as e:
        print(f"Primary query failed: {e}")
        # Fallback to simpler query for compatibility
        simple_query = """
        SELECT dag_id, fileloc, octet_length(data::json::text) as size_bytes,
               round(octet_length(data::json::text) / 1024.0 / 1024.0, 2) AS size_mb,
               last_updated, dag_hash
        FROM serialized_dag WHERE data IS NOT NULL
        ORDER BY octet_length(data::json::text) DESC
        """
        records = postgres_hook.get_records(simple_query)

    if not records:
        return {"total_dags": 0, "message": "No serialized DAG data found"}

    # Process results
    total_dags = len(records)
    suspicious_dags = []
    size_distribution = {
        "<50KB": 0,
        "50-500KB": 0,
        "500KB-1MB": 0,
        "1-5MB": 0,
        ">5MB": 0,
    }

    print(f"\nAnalyzing {total_dags} DAGs...")
    print("NOTE: Size refers to serialized DAG data in database, not source file size")
    print("JSON Size = Actual serialized content size, Storage Size = Database storage overhead")
    print(f"{'DAG ID':<35} {'JSON (MB)':<10} {'Storage (MB)':<12} {'Status':<15} {'Issues':<30}")
    print("-" * 110)

    for record in records:
        dag_id = record[0]
        # Use JSON size (more accurate) - column 5, fallback to storage size - column 8
        if len(record) > 8:
            json_size_mb = float(record[5]) if record[5] is not None else 0
            storage_size_mb = float(record[8]) if record[8] is not None else 0
            size_mb = json_size_mb  # Use JSON size as primary
        elif len(record) > 5:
            size_mb = float(record[5]) if record[5] is not None else 0
            storage_size_mb = None
        else:
            size_mb = float(record[3]) if len(record) > 3 else 0
            storage_size_mb = None

        # Categorize by size
        if size_mb < 0.05:  # 50KB
            size_distribution["<50KB"] += 1
        elif size_mb < 0.5:  # 500KB
            size_distribution["50-500KB"] += 1
        elif size_mb < 1:
            size_distribution["500KB-1MB"] += 1
        elif size_mb < 5:
            size_distribution["1-5MB"] += 1
        else:
            size_distribution[">5MB"] += 1

        # Detect issues
        issues = []
        status = "✅ Normal"

        if size_mb > CRITICAL_DAG_SIZE_MB:
            issues.append("CRITICAL_SIZE")
            status = "🚨 Critical"
        elif size_mb > LARGE_DAG_SIZE_MB:
            issues.append("LARGE_SIZE")
            status = "⚠️ Warning"

        # Additional checks for large serialized DAGs
        if len(record) > 10:
            try:
                # Check for various size thresholds and potential causes
                if size_mb > 50:  # Extremely large
                    issues.append("EXTREMELY_LARGE")
                    status = "🚨 Critical"
                elif size_mb > 20:  # Very large
                    issues.append("VERY_LARGE")
                    status = "⚠️ Warning"
                elif size_mb > 10:  # Large
                    issues.append("LARGE_SERIALIZATION")
                    status = "⚠️ Warning"

                # Try to estimate task count for context
                if len(record) > 10:
                    estimated_tasks = record[10] if record[10] else 0
                    if size_mb > 5 and estimated_tasks < 10:
                        issues.append("LARGE_WITH_FEW_TASKS")
                        status = "⚠️ Warning"

            except Exception:
                pass

        issues_str = ", ".join(issues) if issues else "None"
        storage_size_display = f"{storage_size_mb:.2f}" if storage_size_mb is not None else "N/A"
        print(f"{dag_id:<35} {size_mb:<10.2f} {storage_size_display:<12} {status:<15} {issues_str:<30}")

        if issues:
            suspicious_dags.append(
                {
                    "dag_id": dag_id,
                    "size_mb": size_mb,
                    "issues": issues,
                    "fileloc": record[1] if len(record) > 1 else "Unknown",
                }
            )

    # Calculate statistics
    sizes_mb = []
    for record in records:
        if len(record) > 8:
            json_size_mb = float(record[5]) if record[5] is not None else 0
            sizes_mb.append(json_size_mb)
        elif len(record) > 5:
            sizes_mb.append(float(record[5]) if record[5] is not None else 0)
        else:
            sizes_mb.append(float(record[3]) if len(record) > 3 else 0)
    avg_size_mb = sum(sizes_mb) / len(sizes_mb)
    max_size_mb = max(sizes_mb)

    print(f"\n📊 SUMMARY STATISTICS")
    print(f"Total DAGs: {total_dags}")
    print(f"Average size: {avg_size_mb:.2f} MB")
    print(f"Largest DAG: {max_size_mb:.2f} MB")
    print(f"Suspicious DAGs: {len(suspicious_dags)}")

    return {
        "total_dags": total_dags,
        "avg_size_mb": avg_size_mb,
        "max_size_mb": max_size_mb,
        "suspicious_dags": suspicious_dags,
        "size_distribution": size_distribution,
        "all_dags_by_size": [
            {
                "dag_id": record[0],
                "size_mb": (
                    float(record[5]) if len(record) > 8 and record[5] is not None
                    else float(record[5]) if len(record) > 5 and record[5] is not None
                    else float(record[3]) if len(record) > 3 else 0
                ),
                "storage_size_mb": (
                    float(record[8]) if len(record) > 8 and record[8] is not None else None
                ),
                "fileloc": record[1] if len(record) > 1 else "Unknown",
            }
            for record in records  # Include ALL DAGs, not just top 10
        ],
        "top_10_largest": [
            {
                "dag_id": record[0],
                "size_mb": (
                    float(record[5]) if len(record) > 8 and record[5] is not None
                    else float(record[5]) if len(record) > 5 and record[5] is not None
                    else float(record[3]) if len(record) > 3 else 0
                ),
                "storage_size_mb": (
                    float(record[8]) if len(record) > 8 and record[8] is not None else None
                ),
                "fileloc": record[1] if len(record) > 1 else "Unknown",
            }
            for record in records[:10]  # Keep top 10 for backward compatibility
        ],
    }


@task()
def analyze_dag_performance(**context):
    """DAG performance analysis"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("DAG PERFORMANCE ANALYSIS")
    print("=" * 80)

    query = DiagnosticQueries.get_performance_analysis_query(ANALYSIS_DAYS)
    records = postgres_hook.get_records(query)

    if not records:
        return {"active_dags": 0, "message": "No performance data available"}

    active_dags = len(records)
    performance_issues = []

    print(f"Analyzing {active_dags} active DAGs over last {ANALYSIS_DAYS} days...")
    print(
        f"{'DAG ID':<35} {'Runs':<8} {'Success %':<10} {'Avg Runtime':<12} {'Status':<15}"
    )
    print("-" * 85)

    for record in records[:20]:  # Top 20 most active
        dag_id = record[0]
        total_runs = record[1]
        success_rate = float(record[6]) if record[6] is not None else 0
        avg_runtime = float(record[7]) if record[7] is not None else 0

        # Determine status
        status = "✅ Healthy"
        issues = []

        if success_rate < MIN_SUCCESS_RATE:
            issues.append("LOW_SUCCESS_RATE")
            status = "🚨 Critical"
        elif success_rate < 98:
            status = "⚠️ Warning"

        if avg_runtime > 3600:  # > 1 hour
            issues.append("LONG_RUNTIME")
            if status == "✅ Healthy":
                status = "⚠️ Warning"

        runtime_str = (
            f"{avg_runtime/60:.1f}m"
            if avg_runtime < 3600
            else f"{avg_runtime/3600:.1f}h"
        )

        print(
            f"{dag_id:<35} {total_runs:<8} {success_rate:<10.1f} {runtime_str:<12} {status:<15}"
        )

        if issues:
            performance_issues.append(
                {
                    "dag_id": dag_id,
                    "success_rate": success_rate,
                    "avg_runtime_seconds": avg_runtime,
                    "total_runs": total_runs,
                    "issues": issues,
                }
            )

    # Calculate overall statistics
    success_rates = [float(record[6]) for record in records if record[6] is not None]
    avg_success_rate = sum(success_rates) / len(success_rates) if success_rates else 0

    return {
        "active_dags": active_dags,
        "avg_success_rate": avg_success_rate,
        "performance_issues": performance_issues,
        "performance_summary": [
            {
                "dag_id": record[0],
                "total_runs": int(record[1]),
                "success_rate_percent": float(record[6]) if record[6] else 0,
                "avg_runtime_seconds": float(record[7]) if record[7] else 0,
            }
            for record in records[:10]
        ],
    }


@task()
def analyze_task_failures(**context):
    """task failure analysis"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("TASK FAILURE ANALYSIS")
    print("=" * 80)

    query = DiagnosticQueries.get_task_failure_analysis_query(ANALYSIS_DAYS)
    records = postgres_hook.get_records(query)

    if not records:
        print("No task failures found in the analysis period!")
        return {"total_failing_tasks": 0, "top_failures": [], "dag_failure_summary": {}}

    print(f"Found {len(records)} failing task types in last {ANALYSIS_DAYS} days")
    print(
        f"{'DAG ID':<30} {'Task ID':<25} {'Failures':<10} {'Days':<6} {'Last Failure':<20}"
    )
    print("-" * 95)

    dag_failure_counts = {}
    top_failures = []

    for record in records[:20]:  # Top 20 failing tasks
        dag_id = record[0]
        task_id = record[1]
        failure_count = record[2]
        last_failure = record[3]
        failure_days = record[5]

        print(
            f"{dag_id:<30} {task_id:<25} {failure_count:<10} {failure_days:<6} {str(last_failure)[:19]:<20}"
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

        # Aggregate by DAG
        dag_failure_counts[dag_id] = dag_failure_counts.get(dag_id, 0) + failure_count

    return {
        "total_failing_tasks": len(records),
        "top_failures": top_failures,
        "dag_failure_summary": dict(
            sorted(dag_failure_counts.items(), key=lambda x: x[1], reverse=True)
        ),
    }


@task()
def analyze_serialization_instability(**context):
    """Analyze DAGs with frequent serialization changes"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("SERIALIZATION INSTABILITY ANALYSIS")
    print("=" * 80)

    query = DiagnosticQueries.get_serialization_instability_query(ANALYSIS_DAYS)

    try:
        records = postgres_hook.get_records(query)

        if not records:
            print("No DAGs with serialization instability detected.")
            return {"unstable_dags": 0, "unstable_dag_list": []}

        print(f"Found {len(records)} DAGs with serialization instability")
        print(f"{'DAG ID':<35} {'Hash Vars':<10} {'Updates':<10} {'Avg Size (MB)':<15}")
        print("-" * 75)

        unstable_dags = []
        for record in records:
            dag_id = record[0]
            hash_variations = record[4]
            update_frequency = record[1]
            avg_size_mb = float(record[5]) / 1048576 if record[5] else 0

            print(
                f"{dag_id:<35} {hash_variations:<10} {update_frequency:<10} {avg_size_mb:<15.2f}"
            )

            unstable_dags.append(
                {
                    "dag_id": dag_id,
                    "hash_variations": int(hash_variations),
                    "update_frequency": int(update_frequency),
                    "avg_size_mb": avg_size_mb,
                }
            )

        return {"unstable_dags": len(records), "unstable_dag_list": unstable_dags}

    except Exception as e:
        print(f"Could not analyze serialization instability: {e}")
        return {"unstable_dags": 0, "unstable_dag_list": [], "error": str(e)}


@task()
def collect_system_metrics(**context):
    """Collect system metrics like disk space, memory, CPU, and container statistics"""
    import psutil
    import shutil
    import platform
    import subprocess
    import os
    from datetime import datetime
    
    print("\n" + "=" * 80)
    print("SYSTEM METRICS COLLECTION")
    print("=" * 80)
    
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "collection_errors": []
    }
    
    try:
        # System Information
        print("📋 SYSTEM INFORMATION")
        print("-" * 40)
        
        system_info = {
            "platform": platform.platform(),
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "hostname": platform.node(),
            "python_version": platform.python_version()
        }
        
        metrics["system_info"] = system_info
        
        for key, value in system_info.items():
            print(f"{key.replace('_', ' ').title()}: {value}")
        
    except Exception as e:
        error_msg = f"Error collecting system info: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    try:
        # CPU Information
        print(f"\n🖥️ CPU METRICS")
        print("-" * 40)
        
        cpu_info = {
            "cpu_count_logical": psutil.cpu_count(logical=True),
            "cpu_count_physical": psutil.cpu_count(logical=False),
            "cpu_percent_current": psutil.cpu_percent(interval=1),
            "cpu_percent_per_core": psutil.cpu_percent(interval=1, percpu=True),
            "load_average": os.getloadavg() if hasattr(os, 'getloadavg') else None,
            "cpu_freq": psutil.cpu_freq()._asdict() if psutil.cpu_freq() else None
        }
        
        metrics["cpu_info"] = cpu_info
        
        print(f"Logical CPUs: {cpu_info['cpu_count_logical']}")
        print(f"Physical CPUs: {cpu_info['cpu_count_physical']}")
        print(f"CPU Usage: {cpu_info['cpu_percent_current']:.1f}%")
        if cpu_info['load_average']:
            print(f"Load Average: {cpu_info['load_average']}")
        if cpu_info['cpu_freq']:
            print(f"CPU Frequency: {cpu_info['cpu_freq']['current']:.0f} MHz")
        
    except Exception as e:
        error_msg = f"Error collecting CPU metrics: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    try:
        # Memory Information
        print(f"\n💾 MEMORY METRICS")
        print("-" * 40)
        
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()
        
        memory_info = {
            "total_gb": round(memory.total / (1024**3), 2),
            "available_gb": round(memory.available / (1024**3), 2),
            "used_gb": round(memory.used / (1024**3), 2),
            "percent_used": memory.percent,
            "swap_total_gb": round(swap.total / (1024**3), 2),
            "swap_used_gb": round(swap.used / (1024**3), 2),
            "swap_percent": swap.percent
        }
        
        metrics["memory_info"] = memory_info
        
        print(f"Total Memory: {memory_info['total_gb']} GB")
        print(f"Available Memory: {memory_info['available_gb']} GB")
        print(f"Used Memory: {memory_info['used_gb']} GB ({memory_info['percent_used']:.1f}%)")
        print(f"Swap Total: {memory_info['swap_total_gb']} GB")
        print(f"Swap Used: {memory_info['swap_used_gb']} GB ({memory_info['swap_percent']:.1f}%)")
        
    except Exception as e:
        error_msg = f"Error collecting memory metrics: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    try:
        # Disk Information
        print(f"\n💽 DISK METRICS")
        print("-" * 40)
        
        # Get disk usage for key directories
        disk_info = {}
        key_paths = [
            "/",  # Root filesystem
            "/usr/local/airflow",  # Airflow directory
            "/tmp",  # Temp directory
            "/var/log"  # Log directory
        ]
        
        for path in key_paths:
            try:
                if os.path.exists(path):
                    usage = shutil.disk_usage(path)
                    disk_info[path] = {
                        "total_gb": round(usage.total / (1024**3), 2),
                        "used_gb": round(usage.used / (1024**3), 2),
                        "free_gb": round(usage.free / (1024**3), 2),
                        "percent_used": round((usage.used / usage.total) * 100, 1)
                    }
                    
                    print(f"{path}:")
                    print(f"  Total: {disk_info[path]['total_gb']} GB")
                    print(f"  Used: {disk_info[path]['used_gb']} GB ({disk_info[path]['percent_used']}%)")
                    print(f"  Free: {disk_info[path]['free_gb']} GB")
            except Exception as e:
                disk_info[path] = {"error": str(e)}
                print(f"  Error accessing {path}: {str(e)}")
        
        metrics["disk_info"] = disk_info
        
    except Exception as e:
        error_msg = f"Error collecting disk metrics: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    try:
        # Process Information
        print(f"\n🔄 PROCESS METRICS")
        print("-" * 40)
        
        # Current process info
        current_process = psutil.Process()
        
        process_info = {
            "current_pid": current_process.pid,
            "current_memory_mb": round(current_process.memory_info().rss / (1024**2), 2),
            "current_cpu_percent": current_process.cpu_percent(),
            "total_processes": len(psutil.pids()),
            "airflow_processes": []
        }
        
        # Find Airflow-related processes
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'memory_info', 'cpu_percent']):
            try:
                if proc.info['cmdline'] and any('airflow' in arg.lower() for arg in proc.info['cmdline']):
                    process_info["airflow_processes"].append({
                        "pid": proc.info['pid'],
                        "name": proc.info['name'],
                        "memory_mb": round(proc.info['memory_info'].rss / (1024**2), 2),
                        "cpu_percent": proc.info['cpu_percent']
                    })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        metrics["process_info"] = process_info
        
        print(f"Current Process PID: {process_info['current_pid']}")
        print(f"Current Process Memory: {process_info['current_memory_mb']} MB")
        print(f"Total Processes: {process_info['total_processes']}")
        print(f"Airflow Processes: {len(process_info['airflow_processes'])}")
        
        for proc in process_info["airflow_processes"][:5]:  # Show top 5
            print(f"  PID {proc['pid']}: {proc['memory_mb']} MB, {proc['cpu_percent']}% CPU")
        
    except Exception as e:
        error_msg = f"Error collecting process metrics: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    try:
        # Network Information (basic)
        print(f"\n🌐 NETWORK METRICS")
        print("-" * 40)
        
        network_info = {
            "network_interfaces": len(psutil.net_if_addrs()),
            "network_connections": len(psutil.net_connections()),
        }
        
        # Get network I/O stats
        net_io = psutil.net_io_counters()
        if net_io:
            network_info.update({
                "bytes_sent_mb": round(net_io.bytes_sent / (1024**2), 2),
                "bytes_recv_mb": round(net_io.bytes_recv / (1024**2), 2),
                "packets_sent": net_io.packets_sent,
                "packets_recv": net_io.packets_recv
            })
        
        metrics["network_info"] = network_info
        
        print(f"Network Interfaces: {network_info['network_interfaces']}")
        print(f"Active Connections: {network_info['network_connections']}")
        if 'bytes_sent_mb' in network_info:
            print(f"Bytes Sent: {network_info['bytes_sent_mb']} MB")
            print(f"Bytes Received: {network_info['bytes_recv_mb']} MB")
        
    except Exception as e:
        error_msg = f"Error collecting network metrics: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    try:
        # DAG Folder Analysis
        print(f"\n� DAGT FOLDER ANALYSIS")
        print("-" * 40)
        
        dag_folder_info = {}
        
        # Get DAG folder path
        dags_folder = conf.get("core", "dags_folder", fallback="/usr/local/airflow/dags")
        dag_folder_info["dags_folder_path"] = dags_folder
        
        if os.path.exists(dags_folder):
            print(f"DAGs Folder: {dags_folder}")
            
            # Check for .airflowignore file
            airflowignore_path = os.path.join(dags_folder, ".airflowignore")
            dag_folder_info["has_airflowignore"] = os.path.exists(airflowignore_path)
            
            if dag_folder_info["has_airflowignore"]:
                print("✅ .airflowignore file found")
                try:
                    with open(airflowignore_path, 'r') as f:
                        ignore_content = f.read().strip()
                        dag_folder_info["airflowignore_lines"] = len(ignore_content.split('\n')) if ignore_content else 0
                        dag_folder_info["airflowignore_size"] = len(ignore_content)
                        print(f"  Lines: {dag_folder_info['airflowignore_lines']}")
                        print(f"  Size: {dag_folder_info['airflowignore_size']} bytes")
                except Exception as e:
                    dag_folder_info["airflowignore_error"] = str(e)
                    print(f"  Error reading .airflowignore: {str(e)}")
            else:
                print("⚠️ No .airflowignore file found")
            
            # Scan for files in DAG folder
            try:
                all_files = []
                python_files = []
                non_python_files = []
                total_size_bytes = 0
                python_size_bytes = 0
                non_python_size_bytes = 0
                
                for root, dirs, files in os.walk(dags_folder):
                    # Skip hidden directories and __pycache__
                    dirs[:] = [d for d in dirs if not d.startswith('.') and d != '__pycache__']
                    
                    for file in files:
                        if file.startswith('.'):
                            continue  # Skip hidden files
                        
                        file_path = os.path.join(root, file)
                        relative_path = os.path.relpath(file_path, dags_folder)
                        
                        # Get file size
                        try:
                            file_size = os.path.getsize(file_path)
                            total_size_bytes += file_size
                        except OSError:
                            file_size = 0
                        
                        all_files.append({
                            "path": relative_path,
                            "size_bytes": file_size
                        })
                        
                        if file.endswith('.py'):
                            python_files.append(relative_path)
                            python_size_bytes += file_size
                        else:
                            non_python_files.append(relative_path)
                            non_python_size_bytes += file_size
                
                dag_folder_info["total_files"] = len(all_files)
                dag_folder_info["python_files_count"] = len(python_files)
                dag_folder_info["non_python_files_count"] = len(non_python_files)
                dag_folder_info["non_python_files"] = non_python_files[:20]  # Limit to first 20
                
                # Size information
                dag_folder_info["total_size_bytes"] = total_size_bytes
                dag_folder_info["total_size_mb"] = round(total_size_bytes / (1024 * 1024), 2)
                dag_folder_info["python_size_bytes"] = python_size_bytes
                dag_folder_info["python_size_mb"] = round(python_size_bytes / (1024 * 1024), 2)
                dag_folder_info["non_python_size_bytes"] = non_python_size_bytes
                dag_folder_info["non_python_size_mb"] = round(non_python_size_bytes / (1024 * 1024), 2)
                
                print(f"Total files: {len(all_files)} ({dag_folder_info['total_size_mb']} MB)")
                print(f"Python files: {len(python_files)} ({dag_folder_info['python_size_mb']} MB)")
                print(f"Non-Python files: {len(non_python_files)} ({dag_folder_info['non_python_size_mb']} MB)")
                
                if non_python_files:
                    print("Non-Python files found:")
                    for file in non_python_files[:10]:  # Show first 10
                        print(f"  - {file}")
                    if len(non_python_files) > 10:
                        print(f"  ... and {len(non_python_files) - 10} more")
                
                # Categorize non-Python files by extension
                file_extensions = {}
                for file in non_python_files:
                    ext = os.path.splitext(file)[1].lower() or 'no_extension'
                    file_extensions[ext] = file_extensions.get(ext, 0) + 1
                
                dag_folder_info["file_extensions"] = file_extensions
                
                # Find largest files
                largest_files = sorted(all_files, key=lambda x: x['size_bytes'], reverse=True)[:10]
                dag_folder_info["largest_files"] = [
                    {
                        "path": f["path"],
                        "size_bytes": f["size_bytes"],
                        "size_mb": round(f["size_bytes"] / (1024 * 1024), 2)
                    }
                    for f in largest_files
                ]
                
                if file_extensions:
                    print("File types found:")
                    for ext, count in sorted(file_extensions.items(), key=lambda x: x[1], reverse=True):
                        print(f"  {ext}: {count} files")
                
                # Show largest files if there are any significant ones
                if dag_folder_info["largest_files"]:
                    large_files = [f for f in dag_folder_info["largest_files"] if f["size_mb"] > 0.1]  # > 100KB
                    if large_files:
                        print("Largest files:")
                        for file_info in large_files[:5]:
                            print(f"  - {file_info['path']}: {file_info['size_mb']} MB")
                
            except Exception as e:
                dag_folder_info["scan_error"] = str(e)
                print(f"Error scanning DAG folder: {str(e)}")
        else:
            dag_folder_info["folder_exists"] = False
            print(f"❌ DAGs folder not found: {dags_folder}")
        
        metrics["dag_folder_info"] = dag_folder_info
        
    except Exception as e:
        error_msg = f"Error analyzing DAG folder: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    try:
        # Container/Environment specific information
        print(f"\n🐳 CONTAINER METRICS")
        print("-" * 40)
        
        container_info = {}
        
        # Check if running in container
        if os.path.exists('/.dockerenv'):
            container_info["is_container"] = True
            container_info["container_type"] = "Docker"
        elif os.path.exists('/proc/1/cgroup'):
            with open('/proc/1/cgroup', 'r') as f:
                cgroup_content = f.read()
                if 'docker' in cgroup_content or 'containerd' in cgroup_content:
                    container_info["is_container"] = True
                    container_info["container_type"] = "Container"
                else:
                    container_info["is_container"] = False
        else:
            container_info["is_container"] = False
        
        # Environment variables of interest
        env_vars = {}
        interesting_vars = [
            'AIRFLOW_HOME', 'AIRFLOW__CORE__DAGS_FOLDER', 'AIRFLOW__CORE__EXECUTOR',
            'PYTHONPATH', 'PATH', 'HOSTNAME', 'USER'
        ]
        
        for var in interesting_vars:
            env_vars[var] = os.getenv(var, "Not Set")
        
        container_info["environment_variables"] = env_vars
        
        metrics["container_info"] = container_info
        
        print(f"Running in Container: {container_info.get('is_container', 'Unknown')}")
        if container_info.get('container_type'):
            print(f"Container Type: {container_info['container_type']}")
        
        print("Key Environment Variables:")
        for var, value in env_vars.items():
            display_value = value[:50] + "..." if len(str(value)) > 50 else value
            print(f"  {var}: {display_value}")
        
    except Exception as e:
        error_msg = f"Error collecting container metrics: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    # Summary
    print(f"\n📊 COLLECTION SUMMARY")
    print("-" * 40)
    print(f"Metrics collected: {len([k for k in metrics.keys() if k not in ['timestamp', 'collection_errors']])}")
    print(f"Collection errors: {len(metrics['collection_errors'])}")
    
    # DAG folder summary
    if "dag_folder_info" in metrics:
        dag_folder = metrics["dag_folder_info"]
        total_size_mb = dag_folder.get("total_size_mb", 0)
        print(f"DAG folder analysis:")
        print(f"  .airflowignore: {'✅ Present' if dag_folder.get('has_airflowignore') else '⚠️ Missing'}")
        print(f"  Total size: {total_size_mb} MB")
        non_python = dag_folder.get("non_python_files_count", 0)
        if non_python > 0:
            print(f"  Non-Python files: ⚠️ {non_python} found")
        else:
            print(f"  Non-Python files: ✅ None found")
    
    if metrics['collection_errors']:
        print("Errors encountered:")
        for error in metrics['collection_errors']:
            print(f"  - {error}")
    
    return metrics


@task()
def generate_complete_report(**context):
    """Generate diagnostics report"""

    # Get runtime parameters
    dag_run = context.get("dag_run")
    params = context.get("params", {})

    # Get parameters from DAG run config or fallback to params/defaults
    if dag_run and dag_run.conf:
        analysis_days = dag_run.conf.get(
            "analysis_days", params.get("analysis_days", ANALYSIS_DAYS)
        )
        env_name = dag_run.conf.get("env_name", params.get("env_name", ENV_NAME))
        s3_bucket = dag_run.conf.get("s3_bucket", params.get("s3_bucket", S3_BUCKET))
    else:
        analysis_days = params.get("analysis_days", ANALYSIS_DAYS)
        env_name = params.get("env_name", ENV_NAME)
        s3_bucket = params.get("s3_bucket", S3_BUCKET)

    # Collect all analysis results
    config_analysis = context["task_instance"].xcom_pull(
        task_ids="capture_airflow_configuration"
    )
    dag_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_serialized_dags"
    )
    performance_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_dag_performance"
    )
    failure_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_task_failures"
    )
    instability_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_serialization_instability"
    )
    system_metrics = context["task_instance"].xcom_pull(
        task_ids="collect_system_metrics"
    )

    # Generate diagnostics report
    report_lines = []
    report_lines.append("=" * 100)
    report_lines.append("DAG DIAGNOSTICS REPORT")
    report_lines.append("=" * 100)
    report_lines.append(
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    report_lines.append(f"Environment: {env_name}")
    report_lines.append(f"Analysis Period: {analysis_days} days")
    report_lines.append(f"S3 Bucket: {s3_bucket}")
    
    # Add runtime environment info to header
    try:
        if system_metrics and "system_info" in system_metrics:
            python_version = system_metrics["system_info"].get("python_version", "Unknown")
            platform_info = system_metrics["system_info"].get("platform", "Unknown")
            report_lines.append(f"Runtime: Python {python_version} on {platform_info}")
        else:
            import platform
            python_version = platform.python_version()
            platform_info = platform.platform()
            report_lines.append(f"Runtime: Python {python_version} on {platform_info}")
    except Exception as e:
        report_lines.append(f"Runtime: Error retrieving - {str(e)}")

    # Add Airflow and Python version to header for prominence
    try:
        config = config_analysis.get("configuration", {})
        env_info = config.get("environment_info", {})
        airflow_version = env_info.get("airflow_version", "Unknown")
        report_lines.append(f"Airflow Version: {airflow_version}")
    except Exception as e:
        report_lines.append(f"Airflow Version: Error retrieving - {str(e)}")

    # Add Python version from system metrics
    try:
        if system_metrics and "system_info" in system_metrics:
            python_version = system_metrics["system_info"].get("python_version", "Unknown")
            report_lines.append(f"Python Version: {python_version}")
        else:
            # Fallback to direct detection
            import platform
            python_version = platform.python_version()
            report_lines.append(f"Python Version: {python_version}")
    except Exception as e:
        report_lines.append(f"Python Version: Error retrieving - {str(e)}")

    report_lines.append("")

    # Configuration Analysis Section
    report_lines.append("🔧 AIRFLOW CONFIGURATION ANALYSIS")
    report_lines.append("-" * 50)

    # Configuration change detection
    if config_analysis["config_changed"]:
        report_lines.append("⚠️ CONFIGURATION HAS CHANGED SINCE LAST RUN")
        report_lines.append(f"Previous hash: {config_analysis['previous_hash']}")
        report_lines.append(f"Current hash: {config_analysis['config_hash']}")
    else:
        report_lines.append("✅ No configuration changes detected since last run")

    # Environment information
    try:
        config = config_analysis.get("configuration", {})
        env_info = config.get("environment_info", {})

        # Debug: Print what we're getting
        print(f"DEBUG - config keys: {list(config.keys()) if config else 'None'}")
        print(f"DEBUG - env_info: {env_info}")

        airflow_version = env_info.get("airflow_version", "Unknown")
        executor = env_info.get("executor", "Unknown")
        dags_folder = env_info.get("dags_folder", "Unknown")

        report_lines.append(f"\nEnvironment Information:")
        report_lines.append(f"  Airflow Version: {airflow_version}")
        report_lines.append(f"  Executor: {executor}")
        report_lines.append(f"  DAGs Folder: {dags_folder}")
        
        # Add Python version to environment info
        try:
            if system_metrics and "system_info" in system_metrics:
                python_version = system_metrics["system_info"].get("python_version", "Unknown")
            else:
                import platform
                python_version = platform.python_version()
            report_lines.append(f"  Python Version: {python_version}")
        except Exception:
            report_lines.append(f"  Python Version: Unknown")
    except Exception as e:
        report_lines.append(
            f"\nEnvironment Information: Error retrieving info - {str(e)}"
        )
        print(f"DEBUG - Environment info error: {str(e)}")

    # Configuration issues
    if config_analysis.get("issues"):
        report_lines.append(
            f"\n🚨 CONFIGURATION ISSUES ({len(config_analysis['issues'])}):"
        )
        for issue in config_analysis["issues"]:
            report_lines.append(f"  - {issue}")
    else:
        report_lines.append("\n✅ No critical configuration issues detected")

    # Detailed configuration table
    report_lines.append(f"\nDetailed Configuration Analysis:")
    report_lines.append(f"{'Setting':<40} {'Value':<25} {'Impact':<8} {'Status':<15}")
    report_lines.append("-" * 95)

    try:
        config = config_analysis.get("configuration", {})

        # Define filtered settings for report (excluding sensitive/irrelevant ones)
        stability_settings = {
            "core": [
                "executor",
                "parallelism",
                "max_active_tasks_per_dag",
                "max_active_runs_per_dag",
                "dagbag_import_timeout",
                "dag_file_processor_timeout",
                "killed_task_cleanup_time",
                "dag_discovery_safe_mode",
                "compress_serialized_dags",
                "min_serialized_dag_update_interval",
                "min_serialized_dag_fetch_interval",
            ],
            "scheduler": [
                "dag_dir_list_interval",
                "catchup_by_default",
                "max_dagruns_to_create_per_loop",
                "max_dagruns_per_loop_to_schedule",
                "pool_metrics_interval",
                "orphaned_tasks_check_interval",
                "parsing_processes",
                "file_parsing_sort_mode",
                "use_row_level_locking",
                "max_tis_per_query",
            ],
            "celery": [
                "worker_concurrency",
                "task_track_started",
                "task_publish_retry",
                "worker_enable_remote_control",
            ],
            "webserver": [
                "workers",
                "worker_timeout",
                "worker_refresh_batch_size",
                "reload_on_plugin_change",
            ],
        }

        # Impact levels for display (only for settings included in report)
        impact_levels = {
            "executor": "HIGH",
            "parallelism": "HIGH",
            "max_active_tasks_per_dag": "HIGH",
            "max_active_runs_per_dag": "HIGH",
            "dag_file_processor_timeout": "HIGH",
            "min_serialized_dag_update_interval": "HIGH",
            "dag_dir_list_interval": "HIGH",
            "parsing_processes": "HIGH",
            "worker_concurrency": "HIGH",
        }

        # No sensitive settings in the filtered report
        sensitive_settings = []

        for section_name, settings in stability_settings.items():
            section_config = config.get(section_name, {})

            for setting_name in settings:
                value = section_config.get(setting_name, "NOT_SET")
                impact = impact_levels.get(setting_name, "MEDIUM")

                # Determine status (same logic as in capture function)
                if value == "NOT_SET":
                    status = "🔶 Default"
                elif setting_name in sensitive_settings:
                    status = "✅ Set"
                else:
                    status = analyze_config_setting(
                        section_name, setting_name, value, {"stability_impact": impact}
                    )

                # Format display value
                if setting_name in sensitive_settings and value != "NOT_SET":
                    display_value = "***MASKED***"
                else:
                    display_value = str(value)
                    if len(display_value) > 22:
                        display_value = display_value[:19] + "..."

                # Add to report
                full_name = f"{section_name}.{setting_name}"
                report_lines.append(
                    f"{full_name:<40} {display_value:<25} {impact:<8} {status:<15}"
                )

        # Summary statistics
        report_lines.append("")
        report_lines.append("Configuration Summary:")

        # Count settings by status
        status_counts = {
            "✅ Good": 0,
            "✅ Set": 0,
            "🔶 Default": 0,
            "⚠️ Warning": 0,
            "❌ Error": 0,
        }

        for section_name, settings in stability_settings.items():
            section_config = config.get(section_name, {})
            for setting_name in settings:
                value = section_config.get(setting_name, "NOT_SET")
                impact = impact_levels.get(setting_name, "MEDIUM")

                if value == "NOT_SET":
                    status_counts["🔶 Default"] += 1
                elif setting_name in sensitive_settings:
                    status_counts["✅ Set"] += 1
                else:
                    status = analyze_config_setting(
                        section_name, setting_name, value, {"stability_impact": impact}
                    )
                    if "Good" in status:
                        status_counts["✅ Good"] += 1
                    elif "Set" in status:
                        status_counts["✅ Set"] += 1
                    elif "Default" in status:
                        status_counts["🔶 Default"] += 1
                    elif "⚠️" in status:
                        status_counts["⚠️ Warning"] += 1
                    else:
                        status_counts["❌ Error"] += 1

        for status, count in status_counts.items():
            if count > 0:
                report_lines.append(f"  {status}: {count} settings")

    except Exception as e:
        report_lines.append(f"  Error generating configuration table: {str(e)}")

    report_lines.append(
        f"\nTotal settings analyzed: {config_analysis['total_settings']}"
    )
    report_lines.append("")

    # DAG Analysis Section
    report_lines.append("📊 DAG SERIALIZATION ANALYSIS")
    report_lines.append("-" * 50)
    report_lines.append(
        "NOTE: Sizes refer to serialized DAG data in database, not source file sizes"
    )
    report_lines.append(f"Total DAGs: {dag_analysis['total_dags']}")
    report_lines.append(
        f"Average serialized size: {dag_analysis['avg_size_mb']:.2f} MB"
    )
    report_lines.append(f"Largest serialized DAG: {dag_analysis['max_size_mb']:.2f} MB")
    report_lines.append(f"Suspicious DAGs: {len(dag_analysis['suspicious_dags'])}")

    if dag_analysis["suspicious_dags"]:
        report_lines.append("\n🚨 SUSPICIOUS DAGs:")
        for dag in dag_analysis["suspicious_dags"][:5]:
            report_lines.append(
                f"  - {dag['dag_id']}: {dag['size_mb']:.2f} MB ({', '.join(dag['issues'])})"
            )

    # Size distribution
    report_lines.append("\nSize Distribution:")
    for size_range, count in dag_analysis["size_distribution"].items():
        report_lines.append(f"  {size_range}: {count} DAGs")
    
    # Complete DAG list with sizes
    report_lines.append(f"\nAll DAGs by Size:")
    report_lines.append(f"{'DAG ID':<40} {'JSON (MB)':<10} {'Storage (MB)':<12} {'File Location':<50}")
    report_lines.append("-" * 120)
    
    # Get all DAGs sorted by size (largest first)
    try:
        # Use the complete DAG list from the analysis
        all_dags = dag_analysis.get("all_dags_by_size", dag_analysis.get("top_10_largest", []))
        
        # DAGs are already sorted by size in the query (ORDER BY octet_length(data::text) DESC)
        sorted_dags = all_dags
        
        for dag_info in sorted_dags:
            dag_id = dag_info["dag_id"]
            size_mb = dag_info["size_mb"]
            storage_size_mb = dag_info.get("storage_size_mb")
            fileloc = dag_info.get("fileloc", "Unknown")
            
            # Truncate file location if too long
            if len(fileloc) > 47:
                fileloc = "..." + fileloc[-44:]
            
            storage_display = f"{storage_size_mb:.2f}" if storage_size_mb is not None else "N/A"
            report_lines.append(f"{dag_id:<40} {size_mb:<10.2f} {storage_display:<12} {fileloc:<50}")
            
    except Exception as e:
        report_lines.append(f"Error displaying complete DAG list: {str(e)}")
    
    report_lines.append("")

    # Performance Analysis Section
    report_lines.append("⚡ PERFORMANCE ANALYSIS")
    report_lines.append("-" * 50)
    report_lines.append(f"Active DAGs: {performance_analysis['active_dags']}")
    report_lines.append(
        f"Average success rate: {performance_analysis['avg_success_rate']:.1f}%"
    )

    if performance_analysis["performance_issues"]:
        report_lines.append(
            f"\n⚠️ PERFORMANCE ISSUES ({len(performance_analysis['performance_issues'])}):"
        )
        for issue in performance_analysis["performance_issues"][:5]:
            report_lines.append(
                f"  - {issue['dag_id']}: {issue['success_rate']:.1f}% success ({', '.join(issue['issues'])})"
            )
    report_lines.append("")

    # Failure Analysis Section
    report_lines.append("❌ FAILURE ANALYSIS")
    report_lines.append("-" * 50)
    report_lines.append(
        f"Failing task types: {failure_analysis['total_failing_tasks']}"
    )

    if failure_analysis["top_failures"]:
        report_lines.append("\nTop failing tasks:")
        for failure in failure_analysis["top_failures"][:5]:
            report_lines.append(
                f"  - {failure['dag_id']}.{failure['task_id']}: {failure['failure_count']} failures"
            )
    report_lines.append("")

    # Instability Analysis Section
    report_lines.append("🔄 SERIALIZATION INSTABILITY")
    report_lines.append("-" * 50)
    report_lines.append(f"Unstable DAGs: {instability_analysis['unstable_dags']}")

    if instability_analysis["unstable_dag_list"]:
        report_lines.append("\nMost unstable DAGs:")
        for unstable in instability_analysis["unstable_dag_list"][:5]:
            report_lines.append(
                f"  - {unstable['dag_id']}: {unstable['hash_variations']} hash changes"
            )
    report_lines.append("")

    # System Metrics Section
    report_lines.append("🖥️ SYSTEM STATISTICS")
    report_lines.append("-" * 50)
    
    try:
        if system_metrics and "collection_errors" in system_metrics:
            # System Information
            if "system_info" in system_metrics:
                sys_info = system_metrics["system_info"]
                report_lines.append(f"Platform: {sys_info.get('platform', 'Unknown')}")
                report_lines.append(f"Hostname: {sys_info.get('hostname', 'Unknown')}")
                report_lines.append(f"Python Version: {sys_info.get('python_version', 'Unknown')}")
                report_lines.append(f"System: {sys_info.get('system', 'Unknown')} {sys_info.get('release', '')}")
                report_lines.append(f"Machine: {sys_info.get('machine', 'Unknown')}")
            
            # CPU Metrics
            if "cpu_info" in system_metrics:
                cpu = system_metrics["cpu_info"]
                report_lines.append(f"\nCPU:")
                report_lines.append(f"  Logical CPUs: {cpu.get('cpu_count_logical', 'Unknown')}")
                report_lines.append(f"  Physical CPUs: {cpu.get('cpu_count_physical', 'Unknown')}")
                report_lines.append(f"  CPU Usage: {cpu.get('cpu_percent_current', 'Unknown')}%")
                if cpu.get('load_average'):
                    load_avg = cpu['load_average']
                    report_lines.append(f"  Load Average: {load_avg[0]:.2f}, {load_avg[1]:.2f}, {load_avg[2]:.2f}")
            
            # Memory Metrics
            if "memory_info" in system_metrics:
                mem = system_metrics["memory_info"]
                report_lines.append(f"\nMemory:")
                report_lines.append(f"  Total: {mem.get('total_gb', 'Unknown')} GB")
                report_lines.append(f"  Used: {mem.get('used_gb', 'Unknown')} GB ({mem.get('percent_used', 'Unknown')}%)")
                report_lines.append(f"  Available: {mem.get('available_gb', 'Unknown')} GB")
                if mem.get('swap_total_gb', 0) > 0:
                    report_lines.append(f"  Swap Used: {mem.get('swap_used_gb', 'Unknown')} GB ({mem.get('swap_percent', 'Unknown')}%)")
            
            # Disk Metrics
            if "disk_info" in system_metrics:
                disk = system_metrics["disk_info"]
                report_lines.append(f"\nDisk Usage:")
                for path, info in disk.items():
                    if isinstance(info, dict) and "total_gb" in info:
                        report_lines.append(f"  {path}: {info['used_gb']} GB / {info['total_gb']} GB ({info['percent_used']}% used)")
                    elif isinstance(info, dict) and "error" in info:
                        report_lines.append(f"  {path}: Error - {info['error']}")
            
            # Process Metrics
            if "process_info" in system_metrics:
                proc = system_metrics["process_info"]
                report_lines.append(f"\nProcesses:")
                report_lines.append(f"  Total Processes: {proc.get('total_processes', 'Unknown')}")
                report_lines.append(f"  Airflow Processes: {len(proc.get('airflow_processes', []))}")
                report_lines.append(f"  Current Process Memory: {proc.get('current_memory_mb', 'Unknown')} MB")
                
                # Show top memory-consuming Airflow processes
                airflow_procs = proc.get('airflow_processes', [])
                if airflow_procs:
                    sorted_procs = sorted(airflow_procs, key=lambda x: x.get('memory_mb', 0), reverse=True)
                    report_lines.append(f"  Top Airflow Processes by Memory:")
                    for p in sorted_procs[:3]:
                        report_lines.append(f"    PID {p['pid']}: {p['memory_mb']} MB")
            
            # Container Information
            if "container_info" in system_metrics:
                container = system_metrics["container_info"]
                report_lines.append(f"\nContainer Environment:")
                report_lines.append(f"  Running in Container: {container.get('is_container', 'Unknown')}")
                if container.get('container_type'):
                    report_lines.append(f"  Container Type: {container['container_type']}")
                
                # Key environment variables
                env_vars = container.get('environment_variables', {})
                if env_vars:
                    report_lines.append(f"  Key Environment Variables:")
                    for var in ['AIRFLOW_HOME', 'AIRFLOW__CORE__EXECUTOR']:
                        if var in env_vars and env_vars[var] != "Not Set":
                            value = env_vars[var][:50] + "..." if len(str(env_vars[var])) > 50 else env_vars[var]
                            report_lines.append(f"    {var}: {value}")
            
            # DAG Folder Analysis
            if "dag_folder_info" in system_metrics:
                dag_folder = system_metrics["dag_folder_info"]
                report_lines.append(f"\nDAG Folder Analysis:")
                report_lines.append(f"  DAGs Folder: {dag_folder.get('dags_folder_path', 'Unknown')}")
                
                # .airflowignore status
                if dag_folder.get("has_airflowignore"):
                    lines = dag_folder.get("airflowignore_lines", 0)
                    size = dag_folder.get("airflowignore_size", 0)
                    report_lines.append(f"  .airflowignore: ✅ Present ({lines} lines, {size} bytes)")
                else:
                    report_lines.append(f"  .airflowignore: ⚠️ Missing")
                
                # File analysis
                total_files = dag_folder.get("total_files", 0)
                python_files = dag_folder.get("python_files_count", 0)
                non_python_files = dag_folder.get("non_python_files_count", 0)
                
                # Size information
                total_size_mb = dag_folder.get("total_size_mb", 0)
                python_size_mb = dag_folder.get("python_size_mb", 0)
                non_python_size_mb = dag_folder.get("non_python_size_mb", 0)
                
                report_lines.append(f"  Total Files: {total_files} ({total_size_mb} MB)")
                report_lines.append(f"  Python Files: {python_files} ({python_size_mb} MB)")
                report_lines.append(f"  Non-Python Files: {non_python_files} ({non_python_size_mb} MB)")
                
                # Non-Python files details
                if non_python_files > 0:
                    report_lines.append(f"  ⚠️ Non-Python files detected:")
                    
                    # Show file extensions
                    file_extensions = dag_folder.get("file_extensions", {})
                    for ext, count in sorted(file_extensions.items(), key=lambda x: x[1], reverse=True):
                        report_lines.append(f"    {ext}: {count} files")
                    
                    # Show some example files
                    example_files = dag_folder.get("non_python_files", [])
                    if example_files:
                        report_lines.append(f"  Examples:")
                        for file in example_files[:5]:  # Show first 5
                            report_lines.append(f"    - {file}")
                        if len(example_files) > 5:
                            report_lines.append(f"    ... and {len(example_files) - 5} more")
                
                # Show largest files if folder is substantial
                if total_size_mb > 1:  # Only show if folder is > 1MB
                    largest_files = dag_folder.get("largest_files", [])
                    if largest_files:
                        report_lines.append(f"  Largest Files:")
                        for file_info in largest_files[:5]:  # Show top 5
                            size_mb = file_info["size_mb"]
                            if size_mb > 0.01:  # Only show files > 10KB
                                report_lines.append(f"    - {file_info['path']}: {size_mb} MB")
                        if len([f for f in largest_files if f["size_mb"] > 0.01]) > 5:
                            report_lines.append(f"    ... and more")
            
            # Collection Errors
            if system_metrics.get("collection_errors"):
                report_lines.append(f"\nCollection Issues:")
                for error in system_metrics["collection_errors"][:3]:  # Show first 3 errors
                    report_lines.append(f"  - {error}")
        else:
            report_lines.append("System metrics not available")
            
    except Exception as e:
        report_lines.append(f"Error displaying system metrics: {str(e)}")
    
    report_lines.append("")

    # Recommendations Section
    report_lines.append("💡 RECOMMENDATIONS")
    report_lines.append("-" * 50)

    recommendations = []

    if config_analysis["config_changed"]:
        recommendations.append(
            "Review recent Airflow configuration changes for potential impact"
        )

    if dag_analysis["max_size_mb"] > CRITICAL_DAG_SIZE_MB:
        recommendations.append(
            f"Optimize large DAGs - largest is {dag_analysis['max_size_mb']:.1f}MB"
        )

    if performance_analysis["avg_success_rate"] < MIN_SUCCESS_RATE:
        recommendations.append(
            f"Investigate DAG failures - success rate is {performance_analysis['avg_success_rate']:.1f}%"
        )

    if failure_analysis["total_failing_tasks"] > 10:
        recommendations.append(
            f"Review {failure_analysis['total_failing_tasks']} failing task types"
        )

    if instability_analysis["unstable_dags"] > 0:
        recommendations.append(
            f"Investigate {instability_analysis['unstable_dags']} DAGs with serialization instability"
        )

    # DAG folder recommendations
    if system_metrics and "dag_folder_info" in system_metrics:
        dag_folder = system_metrics["dag_folder_info"]
        
        if not dag_folder.get("has_airflowignore"):
            recommendations.append(
                "Create .airflowignore file to exclude non-DAG files from parsing"
            )
        
        non_python_count = dag_folder.get("non_python_files_count", 0)
        if non_python_count > 0:
            recommendations.append(
                f"Review {non_python_count} non-Python files in DAGs folder - consider moving or ignoring them"
            )
        
        # Check for large DAG folder
        total_size_mb = dag_folder.get("total_size_mb", 0)
        if total_size_mb > 100:  # > 100MB
            recommendations.append(
                f"DAGs folder is large ({total_size_mb} MB) - consider cleanup or archiving old files"
            )
        
        # Check for large non-Python files
        non_python_size_mb = dag_folder.get("non_python_size_mb", 0)
        if non_python_size_mb > 10:  # > 10MB of non-Python files
            recommendations.append(
                f"Non-Python files consume {non_python_size_mb} MB - consider moving large data files elsewhere"
            )

    if not recommendations:
        recommendations.append("System appears healthy - continue monitoring")

    for i, rec in enumerate(recommendations, 1):
        report_lines.append(f"{i}. {rec}")

    report_lines.append("")
    report_lines.append("=" * 100)

    report = "\n".join(report_lines)
    print("\n" + report)

    # Save to S3
    s3_result = save_report_to_s3(report, context, "dag_diagnostics")

    return {
        "report": report,
        "s3_location": s3_result,
        "summary": {
            "total_dags": dag_analysis["total_dags"],
            "suspicious_dags": len(dag_analysis["suspicious_dags"]),
            "performance_issues": len(performance_analysis["performance_issues"]),
            "failing_tasks": failure_analysis["total_failing_tasks"],
            "unstable_dags": instability_analysis["unstable_dags"],
            "config_changed": config_analysis["config_changed"],
            "recommendations_count": len(recommendations),
        },
    }


def save_report_to_s3(
    report_content: str, context: Dict[str, Any], report_type: str
) -> Dict[str, Any]:
    """Save diagnostics report to S3"""
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        # Get S3 bucket from DAG params or fallback to Variable/default
        dag_run = context.get("dag_run")
        if dag_run and dag_run.conf and "s3_bucket" in dag_run.conf:
            s3_bucket = dag_run.conf["s3_bucket"]
        else:
            # Fallback to DAG params or Variable
            params = context.get("params", {})
            s3_bucket = params.get("s3_bucket", S3_BUCKET)

        # Get environment name from params or fallback
        if dag_run and dag_run.conf and "env_name" in dag_run.conf:
            env_name = dag_run.conf["env_name"]
        else:
            env_name = params.get("env_name", ENV_NAME)

        s3_hook = S3Hook(aws_conn_id="aws_default")

        execution_date = context["ds"]
        timestamp = context["ts"].replace(":", "-").replace("+", "_")

        s3_key = f"mwaa_diagnostics/{env_name}/{execution_date}/{report_type}_{timestamp}.txt"

        s3_hook.load_string(
            string_data=report_content, key=s3_key, bucket_name=s3_bucket, replace=True
        )

        s3_location = f"s3://{s3_bucket}/{s3_key}"
        print(f"\n📄 Report saved to S3: {s3_location}")

        return {
            "status": "success",
            "location": s3_location,
            "bucket": s3_bucket,
            "key": s3_key,
        }

    except Exception as e:
        print(f"❌ Failed to save report to S3: {e}")
        return {"status": "failed", "error": str(e)}


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


# =============================================================================
# DAG DEFINITION
# =============================================================================


@dag(
    dag_id=DAG_ID,
    default_args=default_args,
    description="DAG diagnostics system with configuration tracking and extensible analysis",
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=["diagnostics", "mwaa", "monitoring", "configuration"],
    params={"analysis_days": 7, "env_name": ENV_NAME, "s3_bucket": S3_BUCKET},
    doc_md="""
    ## DAG Diagnostics
    
    This DAG provides diagnostics for MWAA environments including:
    - Airflow configuration change detection
    - DAG serialization analysis
    - Performance monitoring
    - Failure pattern analysis
    - Serialization instability detection
    
    ### Parameters:
    - **analysis_days**: Number of days to analyze (default: 7)
    - **env_name**: Environment name for reports (default: from Variable)
    - **s3_bucket**: S3 bucket for saving reports (default: from Variable)
    
    ### Usage:
    Trigger with custom parameters:
    ```json
    {
        "analysis_days": 14,
        "env_name": "MyEnvironment",
        "s3_bucket": "my-diagnostics-bucket"
    }
    ```
    """,
)
def dag_diagnostics_dag():
    """DAG diagnostics workflow"""

    # Setup
    conn_setup = create_postgres_connection()

    # Configuration analysis
    config_capture = capture_airflow_configuration()

    # Core analyses (can run in parallel)
    dag_analysis = analyze_serialized_dags()
    performance_analysis = analyze_dag_performance()
    failure_analysis = analyze_task_failures()
    instability_analysis = analyze_serialization_instability()
    system_metrics = collect_system_metrics()

    # Generate diagnostics report
    report = generate_complete_report()

    # Cleanup
    cleanup = cleanup_postgres_connection()

    # Set dependencies
    conn_setup >> [
        config_capture,
        dag_analysis,
        performance_analysis,
        failure_analysis,
        instability_analysis,
        system_metrics,
    ]
    (
        [
            config_capture,
            dag_analysis,
            performance_analysis,
            failure_analysis,
            instability_analysis,
            system_metrics,
        ]
        >> report
        >> cleanup
    )


# Instantiate the DAG
dag_diagnostics_dag_instance = dag_diagnostics_dag()
