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
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.configuration import conf
from airflow.utils import timezone

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
    "start_date": timezone.datetime(2024, 1, 1),
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

    @staticmethod
    def get_traffic_pattern_hourly_query(analysis_days: int) -> str:
        """Query to analyze hourly traffic patterns"""
        return f"""
        WITH hourly_stats AS (
            SELECT 
                EXTRACT(HOUR FROM execution_date) as hour_of_day,
                EXTRACT(DOW FROM execution_date) as day_of_week,
                COUNT(*) as dag_runs,
                COUNT(DISTINCT dag_id) as unique_dags,
                AVG(CASE 
                    WHEN end_date IS NOT NULL AND start_date IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (end_date - start_date))
                    ELSE NULL 
                END) as avg_duration_seconds
            FROM dag_run
            WHERE execution_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
            GROUP BY hour_of_day, day_of_week
        )
        SELECT 
            hour_of_day,
            SUM(dag_runs) as total_runs,
            AVG(dag_runs) as avg_runs_per_day,
            MAX(dag_runs) as max_runs_in_hour,
            AVG(unique_dags) as avg_unique_dags,
            AVG(avg_duration_seconds) as avg_duration_seconds
        FROM hourly_stats
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """

    @staticmethod
    def get_traffic_pattern_concurrent_query(analysis_days: int) -> str:
        """Query to analyze concurrent DAG run patterns"""
        return f"""
        WITH time_points AS (
            SELECT DISTINCT 
                start_date as time_point
            FROM dag_run
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
            UNION
            SELECT DISTINCT 
                end_date as time_point
            FROM dag_run
            WHERE end_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND end_date IS NOT NULL
        ),
        concurrent_runs AS (
            SELECT 
                tp.time_point,
                COUNT(dr.dag_id) as concurrent_dags
            FROM time_points tp
            LEFT JOIN dag_run dr ON 
                dr.start_date <= tp.time_point 
                AND (dr.end_date >= tp.time_point OR dr.end_date IS NULL)
                AND dr.start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
            GROUP BY tp.time_point
        )
        SELECT 
            MAX(concurrent_dags) as max_concurrent_dags,
            AVG(concurrent_dags) as avg_concurrent_dags,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY concurrent_dags) as p95_concurrent_dags,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY concurrent_dags) as p99_concurrent_dags
        FROM concurrent_runs
        WHERE concurrent_dags > 0
        """

    @staticmethod
    def get_traffic_pattern_dag_frequency_query(analysis_days: int) -> str:
        """Query to analyze DAG execution frequency patterns"""
        return f"""
        WITH dag_schedule_stats AS (
            SELECT 
                dag_id,
                COUNT(*) as total_runs,
                COUNT(DISTINCT DATE(execution_date)) as active_days,
                MIN(execution_date) as first_run,
                MAX(execution_date) as last_run,
                AVG(EXTRACT(EPOCH FROM (end_date - start_date))) as avg_duration_seconds,
                STDDEV(EXTRACT(EPOCH FROM (end_date - start_date))) as stddev_duration_seconds
            FROM dag_run
            WHERE execution_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND end_date IS NOT NULL
                AND start_date IS NOT NULL
            GROUP BY dag_id
        )
        SELECT 
            dag_id,
            total_runs,
            active_days,
            ROUND(total_runs::numeric / NULLIF(active_days, 0), 2) as avg_runs_per_day,
            ROUND(avg_duration_seconds::numeric, 2) as avg_duration_seconds,
            ROUND(stddev_duration_seconds::numeric, 2) as stddev_duration_seconds,
            CASE 
                WHEN stddev_duration_seconds > avg_duration_seconds * 0.5 THEN 'High Variance'
                WHEN stddev_duration_seconds > avg_duration_seconds * 0.3 THEN 'Medium Variance'
                ELSE 'Low Variance'
            END as duration_variance,
            EXTRACT(EPOCH FROM (last_run - first_run)) / 3600 as time_span_hours
        FROM dag_schedule_stats
        WHERE total_runs > 1
        ORDER BY total_runs DESC
        """

    @staticmethod
    def get_database_table_sizes_query() -> str:
        """Query to analyze table sizes and bloat"""
        return """
        SELECT 
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
            pg_total_relation_size(schemaname||'.'||tablename) AS total_size_bytes,
            pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
            pg_relation_size(schemaname||'.'||tablename) AS table_size_bytes,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) AS indexes_size,
            (pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) AS indexes_size_bytes
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        LIMIT 20
        """

    @staticmethod
    def get_database_table_row_counts_query() -> str:
        """Query to get row counts for major Airflow tables"""
        return """
        SELECT 
            'task_instance' as table_name,
            COUNT(*) as row_count,
            COUNT(*) FILTER (WHERE state = 'running') as running_count,
            COUNT(*) FILTER (WHERE state = 'queued') as queued_count,
            COUNT(*) FILTER (WHERE state = 'failed') as failed_count
        FROM task_instance
        UNION ALL
        SELECT 
            'dag_run' as table_name,
            COUNT(*) as row_count,
            COUNT(*) FILTER (WHERE state = 'running') as running_count,
            COUNT(*) FILTER (WHERE state = 'queued') as queued_count,
            COUNT(*) FILTER (WHERE state = 'failed') as failed_count
        FROM dag_run
        UNION ALL
        SELECT 
            'log' as table_name,
            COUNT(*) as row_count,
            NULL as running_count,
            NULL as queued_count,
            NULL as failed_count
        FROM log
        UNION ALL
        SELECT 
            'job' as table_name,
            COUNT(*) as row_count,
            NULL as running_count,
            NULL as queued_count,
            NULL as failed_count
        FROM job
        UNION ALL
        SELECT 
            'xcom' as table_name,
            COUNT(*) as row_count,
            NULL as running_count,
            NULL as queued_count,
            NULL as failed_count
        FROM xcom
        """

    @staticmethod
    def get_database_index_usage_query() -> str:
        """Query to analyze index usage statistics"""
        return """
        SELECT 
            schemaname,
            relname as tablename,
            indexrelname as indexname,
            idx_scan as index_scans,
            idx_tup_read as tuples_read,
            idx_tup_fetch as tuples_fetched,
            pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
            pg_relation_size(indexrelid) as index_size_bytes
        FROM pg_stat_user_indexes
        WHERE schemaname = 'public'
        ORDER BY idx_scan ASC, pg_relation_size(indexrelid) DESC
        LIMIT 30
        """

    @staticmethod
    def get_database_lock_contention_query() -> str:
        """Query to detect current lock contention"""
        return """
        SELECT 
            pg_stat_activity.pid,
            pg_stat_activity.usename,
            pg_stat_activity.application_name,
            pg_stat_activity.state,
            pg_stat_activity.query,
            pg_stat_activity.wait_event_type,
            pg_stat_activity.wait_event,
            age(now(), pg_stat_activity.query_start) as query_duration,
            pg_locks.locktype,
            pg_locks.mode,
            pg_locks.granted
        FROM pg_stat_activity
        JOIN pg_locks ON pg_stat_activity.pid = pg_locks.pid
        WHERE pg_stat_activity.state != 'idle'
            AND pg_locks.granted = false
        ORDER BY pg_stat_activity.query_start
        LIMIT 20
        """

    @staticmethod
    def get_database_connection_stats_query() -> str:
        """Query to analyze database connection usage"""
        return """
        SELECT 
            COUNT(*) as total_connections,
            COUNT(*) FILTER (WHERE state = 'active') as active_connections,
            COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
            COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
            COUNT(*) FILTER (WHERE wait_event_type IS NOT NULL) as waiting_connections,
            MAX(EXTRACT(EPOCH FROM (now() - backend_start))) as max_connection_age_seconds,
            AVG(EXTRACT(EPOCH FROM (now() - backend_start))) as avg_connection_age_seconds
        FROM pg_stat_activity
        WHERE pid != pg_backend_pid()
        """

    @staticmethod
    def get_database_growth_rate_query(analysis_days: int) -> str:
        """Query to estimate database growth rate"""
        return f"""
        WITH table_sizes AS (
            SELECT 
                'task_instance' as table_name,
                COUNT(*) as current_rows,
                COUNT(*) FILTER (WHERE 
                    execution_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                ) as recent_rows
            FROM task_instance
            UNION ALL
            SELECT 
                'dag_run' as table_name,
                COUNT(*) as current_rows,
                COUNT(*) FILTER (WHERE 
                    execution_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                ) as recent_rows
            FROM dag_run
            UNION ALL
            SELECT 
                'log' as table_name,
                COUNT(*) as current_rows,
                COUNT(*) FILTER (WHERE 
                    dttm >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                ) as recent_rows
            FROM log
        )
        SELECT 
            table_name,
            current_rows,
            recent_rows,
            ROUND((recent_rows::numeric / NULLIF({analysis_days}, 0)), 2) as rows_per_day,
            ROUND((recent_rows::numeric / NULLIF(current_rows, 0)) * 100, 2) as recent_percentage
        FROM table_sizes
        """

    @staticmethod
    def get_task_duration_trends_query(analysis_days: int) -> str:
        """Query to analyze task duration trends over time"""
        return f"""
        WITH task_durations AS (
            SELECT 
                dag_id,
                task_id,
                DATE(start_date) as execution_date,
                EXTRACT(EPOCH FROM (end_date - start_date)) as duration_seconds,
                state
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND end_date IS NOT NULL
                AND state = 'success'
                AND EXTRACT(EPOCH FROM (end_date - start_date)) > 0
        ),
        task_stats AS (
            SELECT 
                dag_id,
                task_id,
                COUNT(*) as execution_count,
                AVG(duration_seconds) as avg_duration,
                STDDEV(duration_seconds) as stddev_duration,
                MIN(duration_seconds) as min_duration,
                MAX(duration_seconds) as max_duration,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_seconds) as median_duration,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_seconds) as p95_duration
            FROM task_durations
            GROUP BY dag_id, task_id
            HAVING COUNT(*) >= 5
        ),
        recent_vs_old AS (
            SELECT 
                dag_id,
                task_id,
                AVG(CASE WHEN execution_date >= CURRENT_DATE - INTERVAL '{analysis_days // 2} days' 
                    THEN duration_seconds END) as recent_avg,
                AVG(CASE WHEN execution_date < CURRENT_DATE - INTERVAL '{analysis_days // 2} days' 
                    THEN duration_seconds END) as old_avg
            FROM task_durations
            GROUP BY dag_id, task_id
        )
        SELECT 
            ts.dag_id,
            ts.task_id,
            ts.execution_count,
            ROUND(ts.avg_duration::numeric, 2) as avg_duration_seconds,
            ROUND(ts.stddev_duration::numeric, 2) as stddev_duration_seconds,
            ROUND(ts.min_duration::numeric, 2) as min_duration_seconds,
            ROUND(ts.max_duration::numeric, 2) as max_duration_seconds,
            ROUND(ts.median_duration::numeric, 2) as median_duration_seconds,
            ROUND(ts.p95_duration::numeric, 2) as p95_duration_seconds,
            ROUND(rvo.recent_avg::numeric, 2) as recent_avg_duration,
            ROUND(rvo.old_avg::numeric, 2) as old_avg_duration,
            ROUND((((rvo.recent_avg - rvo.old_avg) / NULLIF(rvo.old_avg, 0)) * 100)::numeric, 2) as duration_change_percent
        FROM task_stats ts
        LEFT JOIN recent_vs_old rvo ON ts.dag_id = rvo.dag_id AND ts.task_id = rvo.task_id
        WHERE rvo.recent_avg IS NOT NULL AND rvo.old_avg IS NOT NULL
        ORDER BY ABS((rvo.recent_avg - rvo.old_avg) / NULLIF(rvo.old_avg, 0)) DESC
        LIMIT 30
        """

    @staticmethod
    def get_task_retry_patterns_query(analysis_days: int) -> str:
        """Query to analyze task retry patterns"""
        return f"""
        WITH task_attempts AS (
            SELECT 
                dag_id,
                task_id,
                run_id,
                try_number,
                state,
                start_date,
                end_date
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
        ),
        retry_stats AS (
            SELECT 
                dag_id,
                task_id,
                COUNT(*) as total_attempts,
                COUNT(DISTINCT run_id) as unique_runs,
                COUNT(*) FILTER (WHERE try_number > 1) as retry_attempts,
                MAX(try_number) as max_retries,
                COUNT(*) FILTER (WHERE state = 'success') as success_count,
                COUNT(*) FILTER (WHERE state = 'failed') as failed_count
            FROM task_attempts
            GROUP BY dag_id, task_id
        )
        SELECT 
            dag_id,
            task_id,
            total_attempts,
            unique_runs,
            retry_attempts,
            max_retries,
            success_count,
            failed_count,
            ROUND((retry_attempts::numeric / NULLIF(total_attempts, 0)) * 100, 2) as retry_rate_percent,
            ROUND((failed_count::numeric / NULLIF(unique_runs, 0)) * 100, 2) as failure_rate_percent
        FROM retry_stats
        WHERE retry_attempts > 0
        ORDER BY retry_attempts DESC
        LIMIT 30
        """

    @staticmethod
    def get_task_queue_depth_query(analysis_days: int) -> str:
        """Query to analyze task queue depth over time"""
        return f"""
        WITH hourly_queue AS (
            SELECT 
                DATE_TRUNC('hour', start_date) as hour,
                COUNT(*) FILTER (WHERE state = 'queued') as queued_count,
                COUNT(*) FILTER (WHERE state = 'running') as running_count,
                COUNT(*) FILTER (WHERE state = 'scheduled') as scheduled_count
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
            GROUP BY DATE_TRUNC('hour', start_date)
        )
        SELECT 
            MAX(queued_count) as max_queued,
            AVG(queued_count) as avg_queued,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY queued_count) as p95_queued,
            MAX(running_count) as max_running,
            AVG(running_count) as avg_running,
            MAX(scheduled_count) as max_scheduled,
            AVG(scheduled_count) as avg_scheduled,
            COUNT(*) as sample_hours
        FROM hourly_queue
        """

    @staticmethod
    def get_executor_queue_lag_query(analysis_days: int) -> str:
        """Query to measure executor queue lag"""
        return f"""
        WITH task_timing AS (
            SELECT 
                dag_id,
                task_id,
                EXTRACT(EPOCH FROM (start_date - queued_dttm)) as queue_to_start_seconds
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND queued_dttm IS NOT NULL
                AND start_date IS NOT NULL
                AND state IN ('success', 'failed', 'running')
                AND EXTRACT(EPOCH FROM (start_date - queued_dttm)) >= 0
        )
        SELECT 
            dag_id,
            task_id,
            COUNT(*) as task_count,
            ROUND(AVG(queue_to_start_seconds)::numeric, 2) as avg_queue_lag_seconds,
            ROUND(MIN(queue_to_start_seconds)::numeric, 2) as min_queue_lag_seconds,
            ROUND(MAX(queue_to_start_seconds)::numeric, 2) as max_queue_lag_seconds,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY queue_to_start_seconds)::numeric, 2) as p95_queue_lag_seconds
        FROM task_timing
        GROUP BY dag_id, task_id
        HAVING COUNT(*) >= 5
        ORDER BY AVG(queue_to_start_seconds) DESC
        LIMIT 30
        """

    @staticmethod
    def get_pool_utilization_query(analysis_days: int) -> str:
        """Query to analyze pool utilization"""
        return f"""
        WITH pool_slots AS (
            SELECT 
                pool,
                slots
            FROM slot_pool
        ),
        pool_usage AS (
            SELECT 
                ti.pool,
                DATE_TRUNC('hour', ti.start_date) as hour,
                COUNT(*) as tasks_in_pool,
                COUNT(*) FILTER (WHERE ti.state = 'running') as running_tasks
            FROM task_instance ti
            WHERE ti.start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND ti.pool IS NOT NULL
                AND ti.pool != 'default_pool'
            GROUP BY ti.pool, DATE_TRUNC('hour', ti.start_date)
        )
        SELECT 
            pu.pool,
            ps.slots as pool_slots,
            COUNT(DISTINCT pu.hour) as active_hours,
            ROUND(AVG(pu.tasks_in_pool)::numeric, 2) as avg_tasks,
            MAX(pu.tasks_in_pool) as max_tasks,
            ROUND(AVG(pu.running_tasks)::numeric, 2) as avg_running,
            MAX(pu.running_tasks) as max_running,
            ROUND(((AVG(pu.running_tasks) / NULLIF(ps.slots, 0)) * 100)::numeric, 2) as avg_utilization_percent,
            ROUND((MAX(pu.running_tasks)::numeric / NULLIF(ps.slots, 0)) * 100, 2) as peak_utilization_percent
        FROM pool_usage pu
        LEFT JOIN pool_slots ps ON pu.pool = ps.pool
        GROUP BY pu.pool, ps.slots
        ORDER BY avg_utilization_percent DESC
        """

    @staticmethod
    def get_zombie_tasks_query() -> str:
        """Query to detect zombie/orphaned tasks"""
        return """
        SELECT 
            dag_id,
            task_id,
            run_id,
            state,
            start_date,
            EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_date)) / 3600 as hours_running,
            hostname,
            pid
        FROM task_instance
        WHERE state = 'running'
            AND start_date < CURRENT_TIMESTAMP - INTERVAL '6 hours'
        ORDER BY start_date
        LIMIT 50
        """

    @staticmethod
    def get_sla_miss_patterns_query(analysis_days: int) -> str:
        """Query to analyze SLA miss patterns"""
        return f"""
        SELECT 
            dag_id,
            task_id,
            COUNT(*) as sla_miss_count,
            MAX(timestamp) as last_sla_miss,
            MIN(timestamp) as first_sla_miss,
            COUNT(DISTINCT DATE(timestamp)) as days_with_misses,
            AVG(EXTRACT(EPOCH FROM (timestamp - execution_date))) as avg_delay_seconds
        FROM sla_miss
        WHERE timestamp >= CURRENT_DATE - INTERVAL '{analysis_days} days'
        GROUP BY dag_id, task_id
        ORDER BY sla_miss_count DESC
        LIMIT 30
        """

    @staticmethod
    def get_execution_time_variance_query(analysis_days: int) -> str:
        """Query to analyze execution time variance"""
        return f"""
        WITH task_durations AS (
            SELECT 
                dag_id,
                task_id,
                EXTRACT(EPOCH FROM (end_date - start_date)) as duration_seconds
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND end_date IS NOT NULL
                AND state = 'success'
                AND EXTRACT(EPOCH FROM (end_date - start_date)) > 0
        )
        SELECT 
            dag_id,
            task_id,
            COUNT(*) as execution_count,
            ROUND(AVG(duration_seconds)::numeric, 2) as avg_duration_seconds,
            ROUND(STDDEV(duration_seconds)::numeric, 2) as stddev_duration_seconds,
            ROUND(MIN(duration_seconds)::numeric, 2) as min_duration_seconds,
            ROUND(MAX(duration_seconds)::numeric, 2) as max_duration_seconds,
            ROUND(((STDDEV(duration_seconds) / NULLIF(AVG(duration_seconds), 0)) * 100)::numeric, 2) as coefficient_of_variation,
            ROUND(((MAX(duration_seconds) - MIN(duration_seconds)) / NULLIF(AVG(duration_seconds), 0))::numeric, 2) as range_ratio
        FROM task_durations
        GROUP BY dag_id, task_id
        HAVING COUNT(*) >= 5 AND STDDEV(duration_seconds) IS NOT NULL
        ORDER BY (STDDEV(duration_seconds) / NULLIF(AVG(duration_seconds), 0)) DESC
        LIMIT 30
        """

    @staticmethod
    def get_schedule_drift_query(analysis_days: int) -> str:
        """Query to analyze schedule drift"""
        return f"""
        WITH dag_timing AS (
            SELECT 
                dag_id,
                execution_date,
                start_date,
                EXTRACT(EPOCH FROM (start_date - execution_date)) as drift_seconds
            FROM dag_run
            WHERE execution_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND start_date IS NOT NULL
                AND state IN ('success', 'failed', 'running')
        )
        SELECT 
            dag_id,
            COUNT(*) as run_count,
            ROUND(AVG(drift_seconds)::numeric, 2) as avg_drift_seconds,
            ROUND(MIN(drift_seconds)::numeric, 2) as min_drift_seconds,
            ROUND(MAX(drift_seconds)::numeric, 2) as max_drift_seconds,
            ROUND(STDDEV(drift_seconds)::numeric, 2) as stddev_drift_seconds,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY drift_seconds)::numeric, 2) as p95_drift_seconds,
            COUNT(*) FILTER (WHERE drift_seconds > 300) as runs_delayed_5min,
            COUNT(*) FILTER (WHERE drift_seconds > 3600) as runs_delayed_1hour
        FROM dag_timing
        GROUP BY dag_id
        HAVING AVG(drift_seconds) > 60
        ORDER BY AVG(drift_seconds) DESC
        LIMIT 30
        """

    @staticmethod
    def get_catchup_backlog_query() -> str:
        """Query to analyze catchup backlog"""
        return """
        SELECT 
            dag_id,
            COUNT(*) as queued_runs,
            MIN(execution_date) as oldest_execution_date,
            MAX(execution_date) as newest_execution_date,
            EXTRACT(EPOCH FROM (MAX(execution_date) - MIN(execution_date))) / 3600 as backlog_span_hours,
            EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MIN(execution_date))) / 3600 as oldest_run_age_hours
        FROM dag_run
        WHERE state = 'queued'
        GROUP BY dag_id
        ORDER BY queued_runs DESC
        LIMIT 30
        """

    @staticmethod
    def get_scheduling_delay_query(analysis_days: int) -> str:
        """Query to analyze execution date vs start date lag"""
        return f"""
        WITH scheduling_lag AS (
            SELECT 
                dag_id,
                execution_date,
                start_date,
                EXTRACT(EPOCH FROM (start_date - execution_date)) as lag_seconds,
                state
            FROM dag_run
            WHERE execution_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND start_date IS NOT NULL
        )
        SELECT 
            dag_id,
            COUNT(*) as total_runs,
            ROUND(AVG(lag_seconds)::numeric, 2) as avg_lag_seconds,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lag_seconds)::numeric, 2) as median_lag_seconds,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lag_seconds)::numeric, 2) as p95_lag_seconds,
            ROUND(MAX(lag_seconds)::numeric, 2) as max_lag_seconds,
            COUNT(*) FILTER (WHERE lag_seconds > 300) as runs_with_5min_lag,
            COUNT(*) FILTER (WHERE lag_seconds > 3600) as runs_with_1hour_lag,
            ROUND((COUNT(*) FILTER (WHERE lag_seconds > 300)::numeric / NULLIF(COUNT(*), 0)) * 100, 2) as pct_delayed_5min
        FROM scheduling_lag
        GROUP BY dag_id
        HAVING AVG(lag_seconds) > 30
        ORDER BY AVG(lag_seconds) DESC
        LIMIT 30
        """

    @staticmethod
    def get_failure_prediction_query(analysis_days: int) -> str:
        """Query to predict task failures based on patterns"""
        return f"""
        WITH task_history AS (
            SELECT 
                dag_id,
                task_id,
                DATE(start_date) as execution_date,
                state,
                CASE WHEN state = 'failed' THEN 1 ELSE 0 END as is_failure
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND state IN ('success', 'failed')
        ),
        failure_stats AS (
            SELECT 
                dag_id,
                task_id,
                COUNT(*) as total_runs,
                SUM(is_failure) as failure_count,
                ROUND((SUM(is_failure)::numeric / COUNT(*)) * 100, 2) as failure_rate,
                COUNT(DISTINCT execution_date) as active_days,
                MAX(execution_date) as last_execution,
                SUM(CASE WHEN execution_date >= CURRENT_DATE - INTERVAL '{analysis_days // 2} days' 
                    THEN is_failure ELSE 0 END) as recent_failures,
                SUM(CASE WHEN execution_date < CURRENT_DATE - INTERVAL '{analysis_days // 2} days' 
                    THEN is_failure ELSE 0 END) as old_failures
            FROM task_history
            GROUP BY dag_id, task_id
        )
        SELECT 
            dag_id,
            task_id,
            total_runs,
            failure_count,
            failure_rate,
            recent_failures,
            old_failures,
            CASE 
                WHEN recent_failures > old_failures AND failure_rate > 10 THEN 'Increasing'
                WHEN recent_failures < old_failures AND failure_rate > 5 THEN 'Decreasing'
                WHEN failure_rate > 20 THEN 'High'
                WHEN failure_rate > 10 THEN 'Moderate'
                ELSE 'Low'
            END as failure_trend,
            ROUND((recent_failures::numeric / NULLIF(old_failures, 0) - 1) * 100, 2) as trend_change_percent
        FROM failure_stats
        WHERE failure_count > 0
        ORDER BY 
            CASE 
                WHEN recent_failures > old_failures THEN 1
                ELSE 2
            END,
            failure_rate DESC
        LIMIT 30
        """

    @staticmethod
    def get_capacity_forecasting_query(analysis_days: int) -> str:
        """Query to forecast when capacity limits will be reached"""
        return f"""
        WITH daily_metrics AS (
            SELECT 
                DATE(start_date) as metric_date,
                COUNT(*) as task_count,
                COUNT(DISTINCT dag_id) as dag_count,
                AVG(EXTRACT(EPOCH FROM (end_date - start_date))) as avg_duration
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND end_date IS NOT NULL
            GROUP BY DATE(start_date)
        ),
        growth_analysis AS (
            SELECT 
                AVG(task_count) as avg_daily_tasks,
                STDDEV(task_count) as stddev_daily_tasks,
                REGR_SLOPE(task_count, EXTRACT(EPOCH FROM metric_date)) as task_growth_rate,
                AVG(dag_count) as avg_daily_dags,
                REGR_SLOPE(dag_count, EXTRACT(EPOCH FROM metric_date)) as dag_growth_rate,
                AVG(avg_duration) as avg_task_duration
            FROM daily_metrics
        )
        SELECT 
            ROUND(avg_daily_tasks::numeric, 2) as current_avg_daily_tasks,
            ROUND((task_growth_rate * 86400)::numeric, 4) as tasks_growth_per_day,
            ROUND(avg_daily_dags::numeric, 2) as current_avg_daily_dags,
            ROUND((dag_growth_rate * 86400)::numeric, 4) as dags_growth_per_day,
            ROUND(avg_task_duration::numeric, 2) as avg_task_duration_seconds
        FROM growth_analysis
        """

    @staticmethod
    def get_performance_degradation_query(analysis_days: int) -> str:
        """Query to detect gradual performance degradation"""
        return f"""
        WITH weekly_performance AS (
            SELECT 
                dag_id,
                task_id,
                DATE_TRUNC('week', start_date) as week,
                AVG(EXTRACT(EPOCH FROM (end_date - start_date))) as avg_duration,
                COUNT(*) as execution_count
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND end_date IS NOT NULL
                AND state = 'success'
                AND EXTRACT(EPOCH FROM (end_date - start_date)) > 0
            GROUP BY dag_id, task_id, DATE_TRUNC('week', start_date)
            HAVING COUNT(*) >= 3
        ),
        trend_analysis AS (
            SELECT 
                dag_id,
                task_id,
                COUNT(*) as week_count,
                AVG(avg_duration) as overall_avg_duration,
                REGR_SLOPE(avg_duration, EXTRACT(EPOCH FROM week)) as duration_trend_slope,
                REGR_R2(avg_duration, EXTRACT(EPOCH FROM week)) as trend_r2
            FROM weekly_performance
            GROUP BY dag_id, task_id
            HAVING COUNT(*) >= 3
        )
        SELECT 
            dag_id,
            task_id,
            ROUND(overall_avg_duration::numeric, 2) as avg_duration_seconds,
            ROUND((duration_trend_slope * 86400 * 7)::numeric, 4) as duration_increase_per_week,
            ROUND(trend_r2::numeric, 4) as trend_confidence,
            CASE 
                WHEN duration_trend_slope > 0 AND trend_r2 > 0.5 THEN 'Degrading'
                WHEN duration_trend_slope < 0 AND trend_r2 > 0.5 THEN 'Improving'
                ELSE 'Stable'
            END as performance_trend
        FROM trend_analysis
        WHERE duration_trend_slope > 0 AND trend_r2 > 0.3
        ORDER BY duration_trend_slope DESC
        LIMIT 30
        """

    @staticmethod
    def get_anomaly_detection_query(analysis_days: int) -> str:
        """Query to detect anomalous execution patterns"""
        return f"""
        WITH task_stats AS (
            SELECT 
                dag_id,
                task_id,
                EXTRACT(EPOCH FROM (end_date - start_date)) as duration_seconds,
                start_date
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND end_date IS NOT NULL
                AND state = 'success'
                AND EXTRACT(EPOCH FROM (end_date - start_date)) > 0
        ),
        statistical_bounds AS (
            SELECT 
                dag_id,
                task_id,
                AVG(duration_seconds) as mean_duration,
                STDDEV(duration_seconds) as stddev_duration,
                COUNT(*) as sample_size
            FROM task_stats
            GROUP BY dag_id, task_id
            HAVING COUNT(*) >= 10 AND STDDEV(duration_seconds) IS NOT NULL
        ),
        anomalies AS (
            SELECT 
                ts.dag_id,
                ts.task_id,
                ts.start_date,
                ts.duration_seconds,
                sb.mean_duration,
                sb.stddev_duration,
                ABS(ts.duration_seconds - sb.mean_duration) / NULLIF(sb.stddev_duration, 0) as z_score
            FROM task_stats ts
            JOIN statistical_bounds sb ON ts.dag_id = sb.dag_id AND ts.task_id = sb.task_id
            WHERE ABS(ts.duration_seconds - sb.mean_duration) / NULLIF(sb.stddev_duration, 0) > 3
        )
        SELECT 
            dag_id,
            task_id,
            COUNT(*) as anomaly_count,
            MAX(start_date) as last_anomaly,
            ROUND(AVG(z_score)::numeric, 2) as avg_z_score,
            ROUND(MAX(z_score)::numeric, 2) as max_z_score,
            ROUND(AVG(duration_seconds)::numeric, 2) as avg_anomaly_duration,
            ROUND(AVG(mean_duration)::numeric, 2) as normal_duration
        FROM anomalies
        GROUP BY dag_id, task_id
        ORDER BY anomaly_count DESC
        LIMIT 30
        """

    @staticmethod
    def get_seasonal_patterns_query(analysis_days: int) -> str:
        """Query to analyze seasonal/cyclical patterns"""
        return f"""
        WITH hourly_patterns AS (
            SELECT 
                EXTRACT(DOW FROM start_date) as day_of_week,
                EXTRACT(HOUR FROM start_date) as hour_of_day,
                COUNT(*) as execution_count,
                AVG(EXTRACT(EPOCH FROM (end_date - start_date))) as avg_duration
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND end_date IS NOT NULL
                AND state = 'success'
            GROUP BY EXTRACT(DOW FROM start_date), EXTRACT(HOUR FROM start_date)
        ),
        pattern_stats AS (
            SELECT 
                day_of_week,
                AVG(execution_count) as avg_executions_per_hour,
                STDDEV(execution_count) as stddev_executions,
                MAX(execution_count) as max_executions,
                MIN(execution_count) as min_executions
            FROM hourly_patterns
            GROUP BY day_of_week
        )
        SELECT 
            day_of_week,
            CASE day_of_week
                WHEN 0 THEN 'Sunday'
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END as day_name,
            ROUND(avg_executions_per_hour::numeric, 2) as avg_executions_per_hour,
            ROUND(stddev_executions::numeric, 2) as stddev_executions,
            max_executions,
            min_executions,
            ROUND(((stddev_executions / NULLIF(avg_executions_per_hour, 0)) * 100)::numeric, 2) as coefficient_of_variation
        FROM pattern_stats
        ORDER BY day_of_week
        """

    @staticmethod
    def get_scheduler_heartbeat_query(analysis_days: int) -> str:
        """Query to analyze scheduler heartbeat and detect hangs"""
        return f"""
        WITH heartbeat_gaps AS (
            SELECT 
                hostname,
                latest_heartbeat,
                LAG(latest_heartbeat) OVER (PARTITION BY hostname ORDER BY latest_heartbeat) as prev_heartbeat,
                EXTRACT(EPOCH FROM (latest_heartbeat - LAG(latest_heartbeat) OVER (PARTITION BY hostname ORDER BY latest_heartbeat))) as gap_seconds
            FROM job
            WHERE job_type = 'SchedulerJob'
                AND latest_heartbeat >= CURRENT_TIMESTAMP - INTERVAL '{analysis_days} days'
        )
        SELECT 
            hostname,
            COUNT(*) as heartbeat_count,
            AVG(gap_seconds) as avg_gap_seconds,
            MAX(gap_seconds) as max_gap_seconds,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY gap_seconds) as p95_gap_seconds,
            COUNT(*) FILTER (WHERE gap_seconds > 60) as gaps_over_1min,
            COUNT(*) FILTER (WHERE gap_seconds > 300) as gaps_over_5min
        FROM heartbeat_gaps
        WHERE gap_seconds IS NOT NULL
        GROUP BY hostname
        """



    @staticmethod
    def get_scheduler_loop_duration_query(analysis_days: int) -> str:
        """Query to analyze scheduler loop performance"""
        return f"""
        WITH scheduler_metrics AS (
            SELECT 
                hostname,
                latest_heartbeat,
                EXTRACT(EPOCH FROM (latest_heartbeat - LAG(latest_heartbeat) OVER (PARTITION BY hostname ORDER BY latest_heartbeat))) as loop_duration
            FROM job
            WHERE job_type = 'SchedulerJob'
                AND latest_heartbeat >= CURRENT_TIMESTAMP - INTERVAL '{analysis_days} days'
        )
        SELECT 
            hostname,
            COUNT(*) as loop_count,
            ROUND(AVG(loop_duration)::numeric, 2) as avg_loop_duration_seconds,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY loop_duration)::numeric, 2) as median_loop_duration,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY loop_duration)::numeric, 2) as p95_loop_duration,
            ROUND(MAX(loop_duration)::numeric, 2) as max_loop_duration,
            COUNT(*) FILTER (WHERE loop_duration > 10) as loops_over_10s
        FROM scheduler_metrics
        WHERE loop_duration IS NOT NULL AND loop_duration > 0
        GROUP BY hostname
        """

    @staticmethod
    def get_queued_tasks_duration_query(analysis_days: int) -> str:
        """Query to analyze how long tasks wait in queue"""
        return f"""
        WITH queue_times AS (
            SELECT 
                dag_id,
                task_id,
                EXTRACT(EPOCH FROM (start_date - queued_dttm)) as queue_duration_seconds
            FROM task_instance
            WHERE queued_dttm IS NOT NULL
                AND start_date IS NOT NULL
                AND queued_dttm >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND EXTRACT(EPOCH FROM (start_date - queued_dttm)) >= 0
        )
        SELECT 
            dag_id,
            task_id,
            COUNT(*) as task_count,
            ROUND(AVG(queue_duration_seconds)::numeric, 2) as avg_queue_seconds,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY queue_duration_seconds)::numeric, 2) as median_queue_seconds,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY queue_duration_seconds)::numeric, 2) as p95_queue_seconds,
            ROUND(MAX(queue_duration_seconds)::numeric, 2) as max_queue_seconds,
            COUNT(*) FILTER (WHERE queue_duration_seconds > 60) as queued_over_1min,
            COUNT(*) FILTER (WHERE queue_duration_seconds > 300) as queued_over_5min
        FROM queue_times
        GROUP BY dag_id, task_id
        HAVING AVG(queue_duration_seconds) > 10
        ORDER BY AVG(queue_duration_seconds) DESC
        LIMIT 30
        """

    @staticmethod
    def get_error_clustering_query(analysis_days: int) -> str:
        """Query to cluster similar errors together"""
        return f"""
        WITH error_patterns AS (
            SELECT 
                dag_id,
                task_id,
                COUNT(*) as occurrence_count,
                MAX(start_date) as last_occurrence,
                MIN(start_date) as first_occurrence,
                COUNT(DISTINCT DATE(start_date)) as days_with_errors
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND state = 'failed'
            GROUP BY dag_id, task_id
        )
        SELECT 
            dag_id,
            task_id,
            'Failed task' as error_snippet,
            occurrence_count,
            last_occurrence,
            first_occurrence,
            days_with_errors,
            CASE 
                WHEN occurrence_count > 50 THEN 'Very High'
                WHEN occurrence_count > 20 THEN 'High'
                WHEN occurrence_count > 10 THEN 'Medium'
                ELSE 'Low'
            END as frequency_category
        FROM error_patterns
        ORDER BY occurrence_count DESC
        LIMIT 30
        """

    @staticmethod
    def get_error_correlation_query(analysis_days: int) -> str:
        """Query to find errors that happen together"""
        return f"""
        WITH error_times AS (
            SELECT 
                dag_id,
                task_id,
                DATE_TRUNC('hour', start_date) as error_hour
            FROM task_fail
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
        ),
        error_pairs AS (
            SELECT 
                e1.dag_id || '.' || e1.task_id as task1,
                e2.dag_id || '.' || e2.task_id as task2,
                COUNT(*) as co_occurrence_count
            FROM error_times e1
            JOIN error_times e2 ON e1.error_hour = e2.error_hour
            WHERE e1.dag_id || '.' || e1.task_id < e2.dag_id || '.' || e2.task_id
            GROUP BY e1.dag_id || '.' || e1.task_id, e2.dag_id || '.' || e2.task_id
            HAVING COUNT(*) >= 3
        )
        SELECT 
            task1,
            task2,
            co_occurrence_count
        FROM error_pairs
        ORDER BY co_occurrence_count DESC
        LIMIT 20
        """

    @staticmethod
    def get_transient_vs_persistent_query(analysis_days: int) -> str:
        """Query to classify errors as transient or persistent"""
        return f"""
        WITH task_attempts AS (
            SELECT 
                dag_id,
                task_id,
                run_id,
                MAX(try_number) as max_try,
                MAX(CASE WHEN state = 'success' THEN 1 ELSE 0 END) as eventually_succeeded
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND try_number > 1
            GROUP BY dag_id, task_id, run_id
        )
        SELECT 
            dag_id,
            task_id,
            COUNT(*) as retry_runs,
            SUM(eventually_succeeded) as successful_after_retry,
            COUNT(*) - SUM(eventually_succeeded) as persistent_failures,
            ROUND((SUM(eventually_succeeded)::numeric / COUNT(*)) * 100, 2) as retry_success_rate,
            CASE 
                WHEN (SUM(eventually_succeeded)::numeric / COUNT(*)) > 0.8 THEN 'Transient'
                WHEN (SUM(eventually_succeeded)::numeric / COUNT(*)) > 0.5 THEN 'Mixed'
                ELSE 'Persistent'
            END as failure_type
        FROM task_attempts
        GROUP BY dag_id, task_id
        HAVING COUNT(*) >= 5
        ORDER BY COUNT(*) DESC
        LIMIT 30
        """

    @staticmethod
    def get_error_time_patterns_query(analysis_days: int) -> str:
        """Query to detect temporal error patterns"""
        return f"""
        WITH hourly_errors AS (
            SELECT 
                EXTRACT(HOUR FROM start_date) as hour_of_day,
                EXTRACT(DOW FROM start_date) as day_of_week,
                COUNT(*) as error_count
            FROM task_fail
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
            GROUP BY EXTRACT(HOUR FROM start_date), EXTRACT(DOW FROM start_date)
        ),
        hourly_stats AS (
            SELECT 
                hour_of_day,
                SUM(error_count) as total_errors,
                AVG(error_count) as avg_errors_per_day,
                MAX(error_count) as max_errors,
                STDDEV(error_count) as stddev_errors
            FROM hourly_errors
            GROUP BY hour_of_day
        )
        SELECT 
            hour_of_day,
            total_errors,
            ROUND(avg_errors_per_day, 2) as avg_errors_per_day,
            max_errors,
            ROUND(stddev_errors, 2) as stddev_errors,
            CASE 
                WHEN total_errors > (SELECT AVG(total_errors) * 1.5 FROM hourly_stats) THEN 'High'
                WHEN total_errors > (SELECT AVG(total_errors) FROM hourly_stats) THEN 'Medium'
                ELSE 'Low'
            END as error_intensity
        FROM hourly_stats
        ORDER BY total_errors DESC
        LIMIT 24
        """

    @staticmethod
    def get_failure_cascade_query(analysis_days: int) -> str:
        """Query to detect downstream failure cascades"""
        return f"""
        WITH dag_failures AS (
            SELECT 
                dag_id,
                run_id,
                MIN(start_date) as first_failure_time,
                COUNT(DISTINCT task_id) as failed_task_count
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND state = 'failed'
            GROUP BY dag_id, run_id
            HAVING COUNT(DISTINCT task_id) >= 3
        )
        SELECT 
            dag_id,
            COUNT(*) as cascade_count,
            ROUND(AVG(failed_task_count), 2) as avg_tasks_per_cascade,
            MAX(failed_task_count) as max_tasks_in_cascade,
            0 as avg_cascade_duration_seconds
        FROM dag_failures
        GROUP BY dag_id
        ORDER BY COUNT(*) DESC
        LIMIT 20
        """

    @staticmethod
    def get_traffic_pattern_task_concurrency_query(analysis_days: int) -> str:
        """Query to analyze task-level concurrency patterns"""
        return f"""
        WITH task_time_points AS (
            SELECT DISTINCT 
                start_date as time_point
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
            UNION
            SELECT DISTINCT 
                end_date as time_point
            FROM task_instance
            WHERE end_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND end_date IS NOT NULL
        ),
        concurrent_tasks AS (
            SELECT 
                tp.time_point,
                COUNT(ti.task_id) as concurrent_tasks
            FROM task_time_points tp
            LEFT JOIN task_instance ti ON 
                ti.start_date <= tp.time_point 
                AND (ti.end_date >= tp.time_point OR ti.end_date IS NULL)
                AND ti.start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND ti.state IN ('running', 'queued')
            GROUP BY tp.time_point
        )
        SELECT 
            MAX(concurrent_tasks) as max_concurrent_tasks,
            AVG(concurrent_tasks) as avg_concurrent_tasks,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY concurrent_tasks) as p95_concurrent_tasks,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY concurrent_tasks) as p99_concurrent_tasks,
            COUNT(*) as sample_points
        FROM concurrent_tasks
        WHERE concurrent_tasks > 0
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
    """Analyze configuration for potential stability issues and conflicts"""
    issues = []

    try:
        # Extract configuration sections
        core_config = config_analysis.get("core", {})
        scheduler_config = config_analysis.get("scheduler", {})
        celery_config = config_analysis.get("celery", {})
        db_config = config_analysis.get("database", {})

        # =================================================================
        # CRITICAL: DAG Processing Timeout Conflicts
        # =================================================================
        
        # Issue: dagbag_import_timeout must be LESS than dag_file_processor_timeout
        # The file processor timeout is the overall timeout for processing a DAG file,
        # while dagbag_import_timeout is for importing the DAG itself
        try:
            dag_file_processor_timeout = int(core_config.get("dag_file_processor_timeout", {}).get("value", "50"))
            dagbag_import_timeout = int(core_config.get("dagbag_import_timeout", {}).get("value", "30"))
            
            if dagbag_import_timeout >= dag_file_processor_timeout:
                issues.append(
                    f"🚨 CRITICAL: dagbag_import_timeout ({dagbag_import_timeout}s) >= "
                    f"dag_file_processor_timeout ({dag_file_processor_timeout}s). "
                    f"DAG imports will timeout before file processing completes. "
                    f"Recommendation: dagbag_import_timeout should be 20-30% less than dag_file_processor_timeout"
                )
            elif dagbag_import_timeout > (dag_file_processor_timeout * 0.8):
                issues.append(
                    f"⚠️ WARNING: dagbag_import_timeout ({dagbag_import_timeout}s) is too close to "
                    f"dag_file_processor_timeout ({dag_file_processor_timeout}s). "
                    f"Leave more buffer to prevent timeout race conditions"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # =================================================================
        # CRITICAL: Scheduler File Processing Interval Conflicts
        # =================================================================
        
        # Issue: min_file_process_interval should be LESS than dag_dir_list_interval
        # The scheduler lists the DAG directory at dag_dir_list_interval, but won't
        # process files more frequently than min_file_process_interval
        try:
            dag_dir_list_interval = int(scheduler_config.get("dag_dir_list_interval", {}).get("value", "300"))
            min_file_process_interval = int(scheduler_config.get("min_file_process_interval", {}).get("value", "30"))
            
            if min_file_process_interval > dag_dir_list_interval:
                issues.append(
                    f"🚨 CRITICAL: min_file_process_interval ({min_file_process_interval}s) > "
                    f"dag_dir_list_interval ({dag_dir_list_interval}s). "
                    f"Scheduler will list DAG directory more frequently than it can process files, "
                    f"causing resource contention and processing delays. "
                    f"Recommendation: min_file_process_interval should be less than dag_dir_list_interval"
                )
            elif min_file_process_interval == dag_dir_list_interval:
                issues.append(
                    f"⚠️ WARNING: min_file_process_interval ({min_file_process_interval}s) equals "
                    f"dag_dir_list_interval ({dag_dir_list_interval}s). "
                    f"This may cause timing conflicts. Set min_file_process_interval lower"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # =================================================================
        # Parallelism and Concurrency Conflicts
        # =================================================================
        
        # Check if serialized DAGs are enabled
        if core_config.get("store_serialized_dags", {}).get("value") not in ["True", "true", "1"]:
            issues.append("⚠️ Serialized DAGs not enabled - may impact performance and scheduler stability")

        # Global parallelism vs max_active_tasks_per_dag
        try:
            parallelism = int(core_config.get("parallelism", {}).get("value", "32"))
            max_active_tasks = int(core_config.get("max_active_tasks_per_dag", {}).get("value", "16"))
            max_active_runs = int(core_config.get("max_active_runs_per_dag", {}).get("value", "16"))
            
            if parallelism < max_active_tasks:
                issues.append(
                    f"⚠️ Global parallelism ({parallelism}) < max_active_tasks_per_dag ({max_active_tasks}). "
                    f"DAGs cannot reach their max task concurrency"
                )
            
            # Check if parallelism is too low for the number of potential concurrent tasks
            potential_concurrent_tasks = max_active_tasks * max_active_runs
            if parallelism < potential_concurrent_tasks:
                issues.append(
                    f"⚠️ Global parallelism ({parallelism}) may be insufficient for "
                    f"max_active_tasks_per_dag ({max_active_tasks}) × max_active_runs_per_dag ({max_active_runs}) = "
                    f"{potential_concurrent_tasks} potential concurrent tasks"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # =================================================================
        # Celery Worker Configuration Conflicts
        # =================================================================
        
        try:
            worker_concurrency = int(celery_config.get("worker_concurrency", {}).get("value", "16"))
            parallelism = int(core_config.get("parallelism", {}).get("value", "32"))
            
            if worker_concurrency > parallelism:
                issues.append(
                    f"⚠️ Celery worker_concurrency ({worker_concurrency}) > core parallelism ({parallelism}). "
                    f"Workers may accept more tasks than the system can handle"
                )
            
            # Check if worker_concurrency is too high for typical worker resources
            if worker_concurrency > 32:
                issues.append(
                    f"⚠️ Very high worker_concurrency ({worker_concurrency}). "
                    f"Ensure workers have sufficient CPU/memory resources"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # =================================================================
        # Database Connection Pool Conflicts
        # =================================================================
        
        try:
            pool_size = int(db_config.get("sql_alchemy_pool_size", {}).get("value", "5"))
            max_overflow = int(db_config.get("sql_alchemy_max_overflow", {}).get("value", "10"))
            parallelism = int(core_config.get("parallelism", {}).get("value", "32"))
            parsing_processes = int(scheduler_config.get("parsing_processes", {}).get("value", "2"))
            
            # Get executor and webserver configuration
            executor = core_config.get("executor", {}).get("value", "SequentialExecutor")
            webserver_workers = int(config_analysis.get("webserver", {}).get("workers", {}).get("value", "4"))
            worker_concurrency = int(celery_config.get("worker_concurrency", {}).get("value", "16"))
            
            # Calculate estimated connection requirements
            # NOTE: Each Airflow process (scheduler, webserver worker, celery worker, etc.) 
            # maintains its own connection pool of size pool_size + max_overflow
            
            # 1. Scheduler components
            scheduler_connections = parsing_processes  # Each parsing process needs connections
            scheduler_connections += 1  # Scheduler main loop
            scheduler_connections += 1  # DagFileProcessorManager
            
            # 2. Webserver
            webserver_connections = webserver_workers  # Each gunicorn worker
            
            # 3. Executor-specific
            executor_connections = 0
            if "Celery" in executor:
                # CeleryExecutor: Each worker process needs connections
                # Estimate: Assume 2-4 worker processes per environment (conservative)
                estimated_celery_workers = 2  # Conservative estimate
                executor_connections = estimated_celery_workers
            elif "Local" in executor:
                # LocalExecutor: Runs tasks in scheduler process, minimal extra connections
                executor_connections = 1
            elif "Kubernetes" in executor:
                # KubernetesExecutor: Minimal connections (pods connect separately)
                executor_connections = 1
            
            # 4. Triggerer (if async tasks are used)
            triggerer_connections = 1
            
            # 5. Buffer for transient connections
            buffer_connections = 2
            
            # Total estimated connections needed across ALL processes
            # IMPORTANT: This is total connections to the database, not per-process pool size
            total_estimated_connections = (
                scheduler_connections + 
                webserver_connections + 
                executor_connections + 
                triggerer_connections + 
                buffer_connections
            )
            
            # Per-process pool calculation
            # Each process has pool_size base + max_overflow burst capacity
            total_pool_per_process = pool_size + max_overflow
            
            # Estimate total possible connections if all processes max out
            # This is a worst-case scenario
            num_processes = (
                1 +  # Scheduler
                parsing_processes +  # DAG processors
                webserver_workers +  # Webserver workers
                executor_connections +  # Executor workers
                1  # Triggerer
            )
            worst_case_connections = num_processes * total_pool_per_process
            
            # Generate warnings based on analysis
            if pool_size < 5:
                issues.append(
                    f"⚠️ Very small database connection pool ({pool_size}). "
                    f"May cause connection exhaustion. Recommended: >= 5"
                )
            
            # Check if pool size is reasonable for the number of processes
            # Rule of thumb: pool_size should be at least 5-10 per major component
            recommended_pool_size = max(5, parsing_processes + 3)
            if pool_size < recommended_pool_size:
                issues.append(
                    f"⚠️ Database pool size ({pool_size}) may be small for {parsing_processes} parsing processes. "
                    f"Each Airflow process maintains its own pool. Recommended: >= {recommended_pool_size}"
                )
            
            # Check total pool capacity per process
            if total_pool_per_process < 10:
                issues.append(
                    f"⚠️ Total pool capacity per process ({pool_size} + {max_overflow} = {total_pool_per_process}) is small. "
                    f"Under load, processes may exhaust connections. Recommended: total >= 10"
                )
            
            # Warn about worst-case scenario
            if worst_case_connections > 100:
                issues.append(
                    f"⚠️ Worst-case database connections: {worst_case_connections} "
                    f"({num_processes} processes × {total_pool_per_process} pool). "
                    f"Verify database max_connections can handle this. "
                    f"Note: Actual usage is typically much lower."
                )
            
            # Check if pool is too large (can cause database connection limits)
            if pool_size > 50:
                issues.append(
                    f"⚠️ Very large connection pool ({pool_size}) per process. "
                    f"With {num_processes} processes, worst-case is {worst_case_connections} connections. "
                    f"Verify database can handle this load."
                )
            
            # Executor-specific warnings
            if "Celery" in executor and pool_size < 10:
                issues.append(
                    f"⚠️ CeleryExecutor with small pool size ({pool_size}). "
                    f"Each Celery worker maintains its own pool. Consider increasing to >= 10."
                )
            
            # Provide context in a separate info message (not an issue, just FYI)
            # This helps users understand the calculation
            connection_breakdown = (
                f"Database Connection Estimate: "
                f"Scheduler={scheduler_connections}, "
                f"Webserver={webserver_connections}, "
                f"Executor={executor_connections}, "
                f"Other={triggerer_connections + buffer_connections}. "
                f"Total estimated: {total_estimated_connections} concurrent connections. "
                f"Per-process pool: {total_pool_per_process}. "
                f"Worst-case (all processes maxed): {worst_case_connections}."
            )
            # Note: We don't add this as an "issue" but it could be logged or included in detailed output
            
        except (ValueError, TypeError, KeyError) as e:
            # Silently skip if we can't parse the values
            pass

        # =================================================================
        # Scheduler Performance Conflicts
        # =================================================================
        
        try:
            parsing_processes = int(scheduler_config.get("parsing_processes", {}).get("value", "2"))
            
            if parsing_processes < 2:
                issues.append(
                    f"⚠️ Low number of DAG parsing processes ({parsing_processes}). "
                    f"May cause DAG processing delays. Recommended: >= 2"
                )
            elif parsing_processes > 8:
                issues.append(
                    f"⚠️ High number of parsing processes ({parsing_processes}). "
                    f"May cause CPU contention and database connection pressure"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # Check scheduler loop intervals
        try:
            dag_dir_list_interval = int(scheduler_config.get("dag_dir_list_interval", {}).get("value", "300"))
            
            if dag_dir_list_interval < 30:
                issues.append(
                    f"⚠️ Very frequent DAG directory scanning ({dag_dir_list_interval}s). "
                    f"May cause excessive I/O and CPU usage"
                )
            elif dag_dir_list_interval > 600:
                issues.append(
                    f"⚠️ Infrequent DAG directory scanning ({dag_dir_list_interval}s). "
                    f"New DAGs may take a long time to be detected"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # =================================================================
        # Task Execution Timeout Conflicts
        # =================================================================
        
        try:
            killed_task_cleanup_time = int(core_config.get("killed_task_cleanup_time", {}).get("value", "60"))
            
            if killed_task_cleanup_time < 30:
                issues.append(
                    f"⚠️ Very short killed_task_cleanup_time ({killed_task_cleanup_time}s). "
                    f"Tasks may be cleaned up before they can gracefully shutdown"
                )
            elif killed_task_cleanup_time > 300:
                issues.append(
                    f"⚠️ Long killed_task_cleanup_time ({killed_task_cleanup_time}s). "
                    f"Zombie tasks may consume resources for extended periods"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # =================================================================
        # Serialization Configuration Conflicts
        # =================================================================
        
        try:
            min_serialized_dag_update_interval = int(core_config.get("min_serialized_dag_update_interval", {}).get("value", "30"))
            min_serialized_dag_fetch_interval = int(core_config.get("min_serialized_dag_fetch_interval", {}).get("value", "10"))
            dag_dir_list_interval = int(scheduler_config.get("dag_dir_list_interval", {}).get("value", "300"))
            
            # Update interval should be less than or equal to dir list interval
            if min_serialized_dag_update_interval > dag_dir_list_interval:
                issues.append(
                    f"⚠️ min_serialized_dag_update_interval ({min_serialized_dag_update_interval}s) > "
                    f"dag_dir_list_interval ({dag_dir_list_interval}s). "
                    f"Serialized DAGs may not update as frequently as directory is scanned"
                )
            
            # Fetch interval should be less than update interval
            if min_serialized_dag_fetch_interval >= min_serialized_dag_update_interval:
                issues.append(
                    f"⚠️ min_serialized_dag_fetch_interval ({min_serialized_dag_fetch_interval}s) >= "
                    f"min_serialized_dag_update_interval ({min_serialized_dag_update_interval}s). "
                    f"Webserver may fetch stale DAG data"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # =================================================================
        # DagRun Creation Limits
        # =================================================================
        
        try:
            max_dagruns_to_create = int(scheduler_config.get("max_dagruns_to_create_per_loop", {}).get("value", "10"))
            max_dagruns_to_schedule = int(scheduler_config.get("max_dagruns_per_loop_to_schedule", {}).get("value", "20"))
            
            if max_dagruns_to_create > max_dagruns_to_schedule:
                issues.append(
                    f"⚠️ max_dagruns_to_create_per_loop ({max_dagruns_to_create}) > "
                    f"max_dagruns_per_loop_to_schedule ({max_dagruns_to_schedule}). "
                    f"Scheduler may create more DagRuns than it can schedule"
                )
        except (ValueError, TypeError, KeyError):
            pass

    except Exception as e:
        issues.append(f"❌ Error analyzing configuration: {str(e)}")

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
            "min_file_process_interval",
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
        "dagbag_import_timeout": "HIGH",
        "dag_file_processor_timeout": "HIGH",
        "store_serialized_dags": "HIGH",
        "min_serialized_dag_update_interval": "HIGH",
        "min_serialized_dag_fetch_interval": "HIGH",
        "dag_dir_list_interval": "HIGH",
        "min_file_process_interval": "HIGH",
        "parsing_processes": "HIGH",
        "worker_concurrency": "HIGH",
        "broker_url": "HIGH",
        "result_backend": "HIGH",
        "sql_alchemy_pool_size": "HIGH",
        "sql_alchemy_max_overflow": "HIGH",
        "max_dagruns_to_create_per_loop": "MEDIUM",
        "max_dagruns_per_loop_to_schedule": "MEDIUM",
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

    # Analyze configuration for conflicts and issues
    print(f"\n🔍 CONFIGURATION CONFLICT ANALYSIS")
    print("-" * 80)
    
    # Prepare config analysis structure for the analyzer
    config_analysis_input = {
        "core": {setting: {"value": value} for setting, value in config_data.get("core", {}).items()},
        "scheduler": {setting: {"value": value} for setting, value in config_data.get("scheduler", {}).items()},
        "celery": {setting: {"value": value} for setting, value in config_data.get("celery", {}).items()},
        "database": {setting: {"value": value} for setting, value in config_data.get("database", {}).items()},
    }
    
    config_issues = analyze_configuration_issues(config_analysis_input)
    
    if config_issues:
        print(f"Found {len(config_issues)} configuration issues:\n")
        for issue in config_issues:
            print(f"  {issue}")
    else:
        print("✅ No configuration conflicts detected")

    return {
        "timestamp": datetime.now().isoformat(),
        "config_hash": config_hash,
        "previous_hash": previous_hash,
        "config_changed": config_changed,
        "configuration": config_data,
        "issues": config_issues,
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
def analyze_error_patterns(**context):
    """Analyze error patterns including clustering, correlation, transient vs persistent, time patterns, and cascades"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("ERROR PATTERN ANALYSIS")
    print("=" * 80)

    error_analysis = {
        "error_clusters": [],
        "error_correlations": [],
        "transient_vs_persistent": [],
        "time_patterns": [],
        "failure_cascades": [],
        "insights": []
    }

    # 1. Error Clustering
    print("\n🔍 ERROR CLUSTERING")
    print("-" * 40)
    
    try:
        clustering_query = DiagnosticQueries.get_error_clustering_query(ANALYSIS_DAYS)
        clustering_records = postgres_hook.get_records(clustering_query)
        
        if clustering_records:
            print(f"{'DAG.Task':<40} {'Count':<8} {'Category':<12} {'Error Snippet':<40}")
            print("-" * 105)
            
            high_frequency_errors = []
            
            for record in clustering_records[:15]:  # Top 15
                dag_id = record[0]
                task_id = record[1]
                error_snippet = record[2][:37] + "..." if len(record[2]) > 37 else record[2]
                count = int(record[3])
                category = record[7]
                
                task_name = f"{dag_id}.{task_id}"
                if len(task_name) > 37:
                    task_name = task_name[:34] + "..."
                
                print(f"{task_name:<40} {count:<8} {category:<12} {error_snippet:<40}")
                
                error_analysis["error_clusters"].append({
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "error_snippet": record[2],
                    "occurrence_count": count,
                    "frequency_category": category
                })
                
                if category in ["Very High", "High"]:
                    high_frequency_errors.append(task_name)
            
            if high_frequency_errors:
                error_analysis["insights"].append(
                    f"🚨 {len(high_frequency_errors)} error patterns with high frequency - prioritize fixes"
                )
                
    except Exception as e:
        print(f"Error in clustering analysis: {e}")
        error_analysis["error_clusters"] = {"error": str(e)}

    # 2. Error Correlation
    print("\n🔗 ERROR CORRELATION")
    print("-" * 40)
    
    try:
        correlation_query = DiagnosticQueries.get_error_correlation_query(ANALYSIS_DAYS)
        correlation_records = postgres_hook.get_records(correlation_query)
        
        if correlation_records:
            print(f"{'Task 1':<40} {'Task 2':<40} {'Co-occur':<10}")
            print("-" * 95)
            
            for record in correlation_records[:10]:  # Top 10
                task1 = record[0]
                task2 = record[1]
                co_occur = int(record[2])
                
                if len(task1) > 37:
                    task1 = task1[:34] + "..."
                if len(task2) > 37:
                    task2 = task2[:34] + "..."
                
                print(f"{task1:<40} {task2:<40} {co_occur:<10}")
                
                error_analysis["error_correlations"].append({
                    "task1": record[0],
                    "task2": record[1],
                    "co_occurrence_count": co_occur
                })
            
            if len(correlation_records) > 5:
                error_analysis["insights"].append(
                    f"💡 {len(correlation_records)} error correlations detected - investigate common causes"
                )
        else:
            print("No significant error correlations detected")
            
    except Exception as e:
        print(f"Error in correlation analysis: {e}")
        error_analysis["error_correlations"] = {"error": str(e)}

    # 3. Transient vs Persistent Failures
    print("\n🔄 TRANSIENT VS PERSISTENT FAILURES")
    print("-" * 40)
    
    try:
        transient_query = DiagnosticQueries.get_transient_vs_persistent_query(ANALYSIS_DAYS)
        transient_records = postgres_hook.get_records(transient_query)
        
        if transient_records:
            print(f"{'DAG.Task':<45} {'Retry Runs':<12} {'Success %':<12} {'Type':<12}")
            print("-" * 85)
            
            transient_count = 0
            persistent_count = 0
            
            for record in transient_records[:15]:  # Top 15
                dag_id = record[0]
                task_id = record[1]
                retry_runs = int(record[2])
                success_rate = float(record[5]) if record[5] else 0
                failure_type = record[6]
                
                task_name = f"{dag_id}.{task_id}"
                if len(task_name) > 42:
                    task_name = task_name[:39] + "..."
                
                print(f"{task_name:<45} {retry_runs:<12} {success_rate:<12.1f} {failure_type:<12}")
                
                error_analysis["transient_vs_persistent"].append({
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "retry_runs": retry_runs,
                    "retry_success_rate": success_rate,
                    "failure_type": failure_type
                })
                
                if failure_type == "Transient":
                    transient_count += 1
                elif failure_type == "Persistent":
                    persistent_count += 1
            
            error_analysis["insights"].append(
                f"💡 {transient_count} transient failures (retryable), {persistent_count} persistent (need fixes)"
            )
                
    except Exception as e:
        print(f"Error in transient analysis: {e}")
        error_analysis["transient_vs_persistent"] = {"error": str(e)}

    # 4. Error Time Patterns
    print("\n⏰ ERROR TIME PATTERNS")
    print("-" * 40)
    
    try:
        time_query = DiagnosticQueries.get_error_time_patterns_query(ANALYSIS_DAYS)
        time_records = postgres_hook.get_records(time_query)
        
        if time_records:
            print(f"{'Hour':<8} {'Total Errors':<15} {'Avg/Day':<12} {'Intensity':<12}")
            print("-" * 50)
            
            high_error_hours = []
            
            for record in time_records[:10]:  # Top 10
                hour = int(record[0])
                total_errors = int(record[1])
                avg_per_day = float(record[2]) if record[2] else 0
                intensity = record[5]
                
                print(f"{hour:02d}:00   {total_errors:<15} {avg_per_day:<12.1f} {intensity:<12}")
                
                error_analysis["time_patterns"].append({
                    "hour_of_day": hour,
                    "total_errors": total_errors,
                    "avg_errors_per_day": avg_per_day,
                    "error_intensity": intensity
                })
                
                if intensity == "High":
                    high_error_hours.append(f"{hour:02d}:00")
            
            if high_error_hours:
                error_analysis["insights"].append(
                    f"⚠️ Error spikes at {', '.join(high_error_hours)} - correlate with workload patterns"
                )
                
    except Exception as e:
        print(f"Error in time pattern analysis: {e}")
        error_analysis["time_patterns"] = {"error": str(e)}

    # 5. Failure Cascades
    print("\n🌊 FAILURE CASCADES")
    print("-" * 40)
    
    try:
        cascade_query = DiagnosticQueries.get_failure_cascade_query(ANALYSIS_DAYS)
        cascade_records = postgres_hook.get_records(cascade_query)
        
        if cascade_records:
            print(f"{'DAG ID':<35} {'Cascades':<12} {'Avg Tasks':<12} {'Max Tasks':<12}")
            print("-" * 75)
            
            for record in cascade_records[:10]:  # Top 10
                dag_id = record[0]
                cascade_count = int(record[1])
                avg_tasks = float(record[2]) if record[2] else 0
                max_tasks = int(record[3]) if record[3] else 0
                
                dag_name = dag_id
                if len(dag_name) > 32:
                    dag_name = dag_name[:29] + "..."
                
                print(f"{dag_name:<35} {cascade_count:<12} {avg_tasks:<12.1f} {max_tasks:<12}")
                
                error_analysis["failure_cascades"].append({
                    "dag_id": dag_id,
                    "cascade_count": cascade_count,
                    "avg_tasks_per_cascade": avg_tasks,
                    "max_tasks_in_cascade": max_tasks
                })
            
            total_cascades = sum(int(r[1]) for r in cascade_records)
            error_analysis["insights"].append(
                f"🚨 {len(cascade_records)} DAGs with failure cascades ({total_cascades} total) - improve error handling"
            )
        else:
            print("✅ No significant failure cascades detected")
            
    except Exception as e:
        print(f"Error in cascade analysis: {e}")
        error_analysis["failure_cascades"] = {"error": str(e)}

    # Summary
    print(f"\n📊 ERROR PATTERN ANALYSIS SUMMARY")
    print("-" * 40)
    print(f"Insights Generated: {len(error_analysis['insights'])}")
    if error_analysis["insights"]:
        print("\nKey Insights:")
        for insight in error_analysis["insights"]:
            print(f"  {insight}")
    else:
        print("✅ No significant error patterns detected")

    return error_analysis


@task()
def analyze_scheduler_performance(**context):
    """Analyze scheduler performance including heartbeat, parsing times, and loop duration"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("SCHEDULER PERFORMANCE ANALYSIS")
    print("=" * 80)

    scheduler_analysis = {
        "heartbeat": [],
        "parsing_times": [],
        "processing_lag": [],
        "loop_duration": [],
        "queue_duration": [],
        "insights": []
    }

    # 1. Scheduler Heartbeat Analysis
    print("\n💓 SCHEDULER HEARTBEAT")
    print("-" * 40)
    
    try:
        heartbeat_query = DiagnosticQueries.get_scheduler_heartbeat_query(ANALYSIS_DAYS)
        heartbeat_records = postgres_hook.get_records(heartbeat_query)
        
        if heartbeat_records:
            print(f"{'Hostname':<30} {'Avg Gap (s)':<12} {'Max Gap (s)':<12} {'>1min':<10}")
            print("-" * 70)
            
            for record in heartbeat_records:
                hostname = record[0]
                avg_gap = float(record[2]) if record[2] else 0
                max_gap = float(record[3]) if record[3] else 0
                gaps_1min = int(record[5]) if record[5] else 0
                
                print(f"{hostname:<30} {avg_gap:<12.1f} {max_gap:<12.1f} {gaps_1min:<10}")
                
                scheduler_analysis["heartbeat"].append({
                    "hostname": hostname,
                    "avg_gap_seconds": avg_gap,
                    "max_gap_seconds": max_gap,
                    "gaps_over_1min": gaps_1min
                })
                
                if max_gap > 300:
                    scheduler_analysis["insights"].append(
                        f"🚨 Scheduler on {hostname} had {max_gap:.0f}s heartbeat gap - possible hang detected"
                    )
                elif gaps_1min > 10:
                    scheduler_analysis["insights"].append(
                        f"⚠️ Scheduler on {hostname} had {gaps_1min} heartbeat gaps >1min - performance issues"
                    )
        else:
            print("✅ Scheduler heartbeat healthy")
            
    except Exception as e:
        print(f"Error analyzing heartbeat: {e}")
        scheduler_analysis["heartbeat"] = {"error": str(e)}

    # 2. DAG Parsing Times - Skipped (dag_processing table not available)
    print("\n📄 DAG PARSING TIMES")
    print("-" * 40)
    print("⚠️ Skipped - dag_processing table not available in this Airflow version")
    scheduler_analysis["parsing_times"] = []

    # 3. File Processing Lag - Skipped (dag_processing table not available)
    print("\n⏱️ FILE PROCESSING LAG")
    print("-" * 40)
    print("⚠️ Skipped - dag_processing table not available in this Airflow version")
    scheduler_analysis["processing_lag"] = []

    # 2. Scheduler Loop Duration
    print("\n🔄 SCHEDULER LOOP DURATION")
    print("-" * 40)
    
    try:
        loop_query = DiagnosticQueries.get_scheduler_loop_duration_query(ANALYSIS_DAYS)
        loop_records = postgres_hook.get_records(loop_query)
        
        if loop_records:
            print(f"{'Hostname':<30} {'Avg (s)':<10} {'P95 (s)':<10} {'>10s':<10}")
            print("-" * 65)
            
            for record in loop_records:
                hostname = record[0]
                avg_duration = float(record[2]) if record[2] else 0
                p95_duration = float(record[4]) if record[4] else 0
                loops_10s = int(record[6]) if record[6] else 0
                
                print(f"{hostname:<30} {avg_duration:<10.2f} {p95_duration:<10.2f} {loops_10s:<10}")
                
                scheduler_analysis["loop_duration"].append({
                    "hostname": hostname,
                    "avg_duration_seconds": avg_duration,
                    "p95_duration_seconds": p95_duration,
                    "loops_over_10s": loops_10s
                })
                
                if p95_duration > 10:
                    scheduler_analysis["insights"].append(
                        f"⚠️ Scheduler on {hostname} has P95 loop duration {p95_duration:.1f}s - performance bottleneck"
                    )
        else:
            print("No scheduler loop data available")
            
    except Exception as e:
        print(f"Error analyzing loop duration: {e}")
        scheduler_analysis["loop_duration"] = {"error": str(e)}

    # 3. Queued Tasks Duration
    print("\n⏳ QUEUED TASKS DURATION")
    print("-" * 40)
    
    try:
        queue_query = DiagnosticQueries.get_queued_tasks_duration_query(ANALYSIS_DAYS)
        queue_records = postgres_hook.get_records(queue_query)
        
        if queue_records:
            print(f"{'DAG.Task':<45} {'Avg (s)':<10} {'P95 (s)':<10}")
            print("-" * 70)
            
            long_queue = []
            
            for record in queue_records[:15]:  # Top 15
                dag_id = record[0]
                task_id = record[1]
                avg_queue = float(record[3]) if record[3] else 0
                p95_queue = float(record[5]) if record[5] else 0
                
                task_name = f"{dag_id}.{task_id}"
                if len(task_name) > 42:
                    task_name = task_name[:39] + "..."
                
                print(f"{task_name:<45} {avg_queue:<10.1f} {p95_queue:<10.1f}")
                
                scheduler_analysis["queue_duration"].append({
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "avg_queue_seconds": avg_queue,
                    "p95_queue_seconds": p95_queue
                })
                
                if p95_queue > 300:
                    long_queue.append(task_name)
            
            if long_queue:
                scheduler_analysis["insights"].append(
                    f"⚠️ {len(long_queue)} tasks wait >5min in queue - increase worker capacity"
                )
        else:
            print("✅ No significant queue delays")
            
    except Exception as e:
        print(f"Error analyzing queue duration: {e}")
        scheduler_analysis["queue_duration"] = {"error": str(e)}

    # Summary
    print(f"\n📊 SCHEDULER PERFORMANCE SUMMARY")
    print("-" * 40)
    print(f"Insights Generated: {len(scheduler_analysis['insights'])}")
    if scheduler_analysis["insights"]:
        print("\nKey Insights:")
        for insight in scheduler_analysis["insights"]:
            print(f"  {insight}")
    else:
        print("✅ Scheduler performing well")

    return scheduler_analysis


@task()
def analyze_predictive_patterns(**context):
    """Predictive analytics: failure prediction, capacity forecasting, degradation trends, anomalies, seasonal patterns"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("PREDICTIVE ANALYTICS")
    print("=" * 80)

    predictive_analysis = {
        "failure_predictions": [],
        "capacity_forecast": {},
        "performance_degradation": [],
        "anomalies": [],
        "seasonal_patterns": [],
        "insights": []
    }

    # 1. Failure Prediction
    print("\n🔮 FAILURE PREDICTION")
    print("-" * 40)
    
    try:
        failure_query = DiagnosticQueries.get_failure_prediction_query(ANALYSIS_DAYS)
        failure_records = postgres_hook.get_records(failure_query)
        
        if failure_records:
            print(f"{'DAG.Task':<45} {'Fail %':<10} {'Trend':<12} {'Risk':<10}")
            print("-" * 80)
            
            high_risk_tasks = []
            increasing_failures = []
            
            for record in failure_records[:15]:  # Top 15
                dag_id = record[0]
                task_id = record[1]
                failure_rate = float(record[4]) if record[4] else 0
                trend = record[7] if record[7] else "Unknown"
                
                task_name = f"{dag_id}.{task_id}"
                if len(task_name) > 42:
                    task_name = task_name[:39] + "..."
                
                # Determine risk level
                if trend == "Increasing" and failure_rate > 20:
                    risk = "🚨 Critical"
                    high_risk_tasks.append(task_name)
                    increasing_failures.append(task_name)
                elif trend == "Increasing":
                    risk = "⚠️ High"
                    increasing_failures.append(task_name)
                elif failure_rate > 30:
                    risk = "⚠️ High"
                    high_risk_tasks.append(task_name)
                elif failure_rate > 15:
                    risk = "⚠️ Medium"
                else:
                    risk = "💡 Low"
                
                print(f"{task_name:<45} {failure_rate:<10.1f} {trend:<12} {risk:<10}")
                
                predictive_analysis["failure_predictions"].append({
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "failure_rate": failure_rate,
                    "trend": trend,
                    "risk_level": risk
                })
            
            if increasing_failures:
                predictive_analysis["insights"].append(
                    f"🚨 {len(increasing_failures)} tasks showing increasing failure rates - investigate before failures escalate"
                )
            
            if high_risk_tasks:
                predictive_analysis["insights"].append(
                    f"⚠️ {len(high_risk_tasks)} high-risk tasks identified - proactive intervention recommended"
                )
        else:
            print("✅ No high-risk failure patterns detected")
            
    except Exception as e:
        print(f"Error in failure prediction: {e}")
        predictive_analysis["failure_predictions"] = {"error": str(e)}

    # 2. Capacity Forecasting
    print("\n📈 CAPACITY FORECASTING")
    print("-" * 40)
    
    try:
        capacity_query = DiagnosticQueries.get_capacity_forecasting_query(ANALYSIS_DAYS)
        capacity_records = postgres_hook.get_records(capacity_query)
        
        if capacity_records and capacity_records[0]:
            record = capacity_records[0]
            current_tasks = float(record[0]) if record[0] else 0
            task_growth = float(record[1]) if record[1] else 0
            current_dags = float(record[2]) if record[2] else 0
            dag_growth = float(record[3]) if record[3] else 0
            
            print(f"Current Daily Tasks: {current_tasks:.0f}")
            print(f"Task Growth Rate: {task_growth:+.2f} tasks/day")
            print(f"Current Daily DAGs: {current_dags:.0f}")
            print(f"DAG Growth Rate: {dag_growth:+.2f} DAGs/day")
            
            predictive_analysis["capacity_forecast"] = {
                "current_daily_tasks": current_tasks,
                "task_growth_per_day": task_growth,
                "current_daily_dags": current_dags,
                "dag_growth_per_day": dag_growth
            }
            
            # Forecast when limits might be reached
            try:
                parallelism = int(conf.get("core", "parallelism", fallback="32"))
                
                if task_growth > 0:
                    # Estimate days until 80% capacity
                    capacity_80pct = parallelism * 0.8 * 24  # 80% of daily capacity
                    if current_tasks < capacity_80pct:
                        days_to_80pct = (capacity_80pct - current_tasks) / task_growth
                        print(f"\nCapacity Forecast:")
                        print(f"  Days to 80% capacity: {days_to_80pct:.0f} days")
                        
                        if days_to_80pct < 30:
                            predictive_analysis["insights"].append(
                                f"🚨 Capacity limit approaching - will reach 80% in {days_to_80pct:.0f} days"
                            )
                        elif days_to_80pct < 90:
                            predictive_analysis["insights"].append(
                                f"⚠️ Plan capacity increase - will reach 80% in {days_to_80pct:.0f} days"
                            )
                elif task_growth < -10:
                    predictive_analysis["insights"].append(
                        f"💡 Workload decreasing - consider right-sizing resources"
                    )
            except Exception:
                pass
                
    except Exception as e:
        print(f"Error in capacity forecasting: {e}")
        predictive_analysis["capacity_forecast"] = {"error": str(e)}

    # 3. Performance Degradation Trends
    print("\n📉 PERFORMANCE DEGRADATION TRENDS")
    print("-" * 40)
    
    try:
        degradation_query = DiagnosticQueries.get_performance_degradation_query(ANALYSIS_DAYS)
        degradation_records = postgres_hook.get_records(degradation_query)
        
        if degradation_records:
            print(f"{'DAG.Task':<45} {'Avg (s)':<10} {'+/week (s)':<12} {'Confidence':<12}")
            print("-" * 85)
            
            degrading_tasks = []
            
            for record in degradation_records[:15]:  # Top 15
                dag_id = record[0]
                task_id = record[1]
                avg_duration = float(record[2]) if record[2] else 0
                increase_per_week = float(record[3]) if record[3] else 0
                confidence = float(record[4]) if record[4] else 0
                
                task_name = f"{dag_id}.{task_id}"
                if len(task_name) > 42:
                    task_name = task_name[:39] + "..."
                
                print(f"{task_name:<45} {avg_duration:<10.1f} {increase_per_week:>+11.2f} {confidence:<12.2f}")
                
                predictive_analysis["performance_degradation"].append({
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "avg_duration_seconds": avg_duration,
                    "increase_per_week_seconds": increase_per_week,
                    "trend_confidence": confidence
                })
                
                if confidence > 0.7:
                    degrading_tasks.append(task_name)
            
            if degrading_tasks:
                predictive_analysis["insights"].append(
                    f"⚠️ {len(degrading_tasks)} tasks showing consistent performance degradation - investigate root causes"
                )
        else:
            print("✅ No significant performance degradation trends detected")
            
    except Exception as e:
        print(f"Error analyzing degradation trends: {e}")
        predictive_analysis["performance_degradation"] = {"error": str(e)}

    # 4. Anomaly Detection
    print("\n🎯 ANOMALY DETECTION")
    print("-" * 40)
    
    try:
        anomaly_query = DiagnosticQueries.get_anomaly_detection_query(ANALYSIS_DAYS)
        anomaly_records = postgres_hook.get_records(anomaly_query)
        
        if anomaly_records:
            print(f"{'DAG.Task':<45} {'Anomalies':<12} {'Max Z-Score':<12}")
            print("-" * 75)
            
            for record in anomaly_records[:15]:  # Top 15
                dag_id = record[0]
                task_id = record[1]
                anomaly_count = int(record[2])
                max_z_score = float(record[5]) if record[5] else 0
                
                task_name = f"{dag_id}.{task_id}"
                if len(task_name) > 42:
                    task_name = task_name[:39] + "..."
                
                print(f"{task_name:<45} {anomaly_count:<12} {max_z_score:<12.1f}")
                
                predictive_analysis["anomalies"].append({
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "anomaly_count": anomaly_count,
                    "max_z_score": max_z_score
                })
            
            total_anomalies = sum(int(r[2]) for r in anomaly_records)
            predictive_analysis["insights"].append(
                f"💡 {len(anomaly_records)} tasks with anomalous behavior ({total_anomalies} anomalies detected)"
            )
        else:
            print("✅ No significant anomalies detected")
            
    except Exception as e:
        print(f"Error in anomaly detection: {e}")
        predictive_analysis["anomalies"] = {"error": str(e)}

    # 5. Seasonal Pattern Analysis
    print("\n📅 SEASONAL PATTERN ANALYSIS")
    print("-" * 40)
    
    try:
        seasonal_query = DiagnosticQueries.get_seasonal_patterns_query(ANALYSIS_DAYS)
        seasonal_records = postgres_hook.get_records(seasonal_query)
        
        if seasonal_records:
            print(f"{'Day':<12} {'Avg/Hour':<12} {'Max':<10} {'Min':<10} {'Variance':<10}")
            print("-" * 60)
            
            for record in seasonal_records:
                day_name = record[1]
                avg_per_hour = float(record[2]) if record[2] else 0
                max_exec = int(record[4]) if record[4] else 0
                min_exec = int(record[5]) if record[5] else 0
                cv = float(record[6]) if record[6] else 0
                
                print(f"{day_name:<12} {avg_per_hour:<12.1f} {max_exec:<10} {min_exec:<10} {cv:<10.1f}%")
                
                predictive_analysis["seasonal_patterns"].append({
                    "day_name": day_name,
                    "avg_executions_per_hour": avg_per_hour,
                    "max_executions": max_exec,
                    "min_executions": min_exec,
                    "coefficient_of_variation": cv
                })
            
            # Identify high variance days
            high_variance_days = [r[1] for r in seasonal_records if r[6] and float(r[6]) > 50]
            if high_variance_days:
                predictive_analysis["insights"].append(
                    f"💡 High workload variance on {', '.join(high_variance_days)} - consider dynamic scaling"
                )
                
    except Exception as e:
        print(f"Error analyzing seasonal patterns: {e}")
        predictive_analysis["seasonal_patterns"] = {"error": str(e)}

    # Summary
    print(f"\n📊 PREDICTIVE ANALYTICS SUMMARY")
    print("-" * 40)
    print(f"Insights Generated: {len(predictive_analysis['insights'])}")
    if predictive_analysis["insights"]:
        print("\nKey Insights:")
        for insight in predictive_analysis["insights"]:
            print(f"  {insight}")
    else:
        print("✅ No significant predictive concerns detected")

    return predictive_analysis


@task()
def analyze_sla_and_timing(**context):
    """Analyze SLA misses, execution variance, schedule drift, and timing delays"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("SLA & TIMING ANALYSIS")
    print("=" * 80)

    sla_analysis = {
        "sla_misses": [],
        "execution_variance": [],
        "schedule_drift": [],
        "catchup_backlog": [],
        "scheduling_delay": [],
        "insights": []
    }

    # 1. SLA Miss Patterns
    print("\n⏰ SLA MISS PATTERNS")
    print("-" * 40)
    
    try:
        sla_query = DiagnosticQueries.get_sla_miss_patterns_query(ANALYSIS_DAYS)
        sla_records = postgres_hook.get_records(sla_query)
        
        if sla_records:
            print(f"{'DAG.Task':<45} {'Misses':<10} {'Days':<8} {'Avg Delay':<12}")
            print("-" * 80)
            
            for record in sla_records[:15]:  # Top 15
                dag_id = record[0]
                task_id = record[1]
                miss_count = int(record[2])
                days_with_misses = int(record[5])
                avg_delay = float(record[6]) if record[6] else 0
                
                task_name = f"{dag_id}.{task_id}"
                if len(task_name) > 42:
                    task_name = task_name[:39] + "..."
                
                delay_str = f"{avg_delay/60:.1f}m" if avg_delay < 3600 else f"{avg_delay/3600:.1f}h"
                
                print(f"{task_name:<45} {miss_count:<10} {days_with_misses:<8} {delay_str:<12}")
                
                sla_analysis["sla_misses"].append({
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "miss_count": miss_count,
                    "days_with_misses": days_with_misses,
                    "avg_delay_seconds": avg_delay
                })
            
            total_misses = sum(int(r[2]) for r in sla_records)
            sla_analysis["insights"].append(
                f"⚠️ {len(sla_records)} tasks with SLA misses ({total_misses} total misses)"
            )
        else:
            print("✅ No SLA misses detected")
            
    except Exception as e:
        print(f"Error analyzing SLA misses: {e}")
        sla_analysis["sla_misses"] = {"error": str(e)}

    # 2. Execution Time Variance
    print("\n📊 EXECUTION TIME VARIANCE")
    print("-" * 40)
    
    try:
        variance_query = DiagnosticQueries.get_execution_time_variance_query(ANALYSIS_DAYS)
        variance_records = postgres_hook.get_records(variance_query)
        
        if variance_records:
            print(f"{'DAG.Task':<45} {'Avg (s)':<10} {'StdDev':<10} {'CV %':<10}")
            print("-" * 80)
            
            high_variance_tasks = []
            
            for record in variance_records[:15]:  # Top 15
                dag_id = record[0]
                task_id = record[1]
                avg_duration = float(record[3]) if record[3] else 0
                stddev = float(record[4]) if record[4] else 0
                cv = float(record[7]) if record[7] else 0
                
                task_name = f"{dag_id}.{task_id}"
                if len(task_name) > 42:
                    task_name = task_name[:39] + "..."
                
                print(f"{task_name:<45} {avg_duration:<10.1f} {stddev:<10.1f} {cv:<10.1f}")
                
                sla_analysis["execution_variance"].append({
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "avg_duration_seconds": avg_duration,
                    "stddev_duration_seconds": stddev,
                    "coefficient_of_variation": cv
                })
                
                if cv > 50:
                    high_variance_tasks.append(task_name)
            
            if high_variance_tasks:
                sla_analysis["insights"].append(
                    f"⚠️ {len(high_variance_tasks)} tasks with high execution variance (CV >50%) - unpredictable duration"
                )
                
    except Exception as e:
        print(f"Error analyzing execution variance: {e}")
        sla_analysis["execution_variance"] = {"error": str(e)}

    # 3. Schedule Drift Analysis
    print("\n🎯 SCHEDULE DRIFT")
    print("-" * 40)
    
    try:
        drift_query = DiagnosticQueries.get_schedule_drift_query(ANALYSIS_DAYS)
        drift_records = postgres_hook.get_records(drift_query)
        
        if drift_records:
            print(f"{'DAG ID':<35} {'Avg Drift':<12} {'P95 Drift':<12} {'>5min':<10}")
            print("-" * 75)
            
            significant_drift = []
            
            for record in drift_records[:15]:  # Top 15
                dag_id = record[0]
                avg_drift = float(record[2]) if record[2] else 0
                p95_drift = float(record[6]) if record[6] else 0
                delayed_5min = int(record[7]) if record[7] else 0
                
                dag_name = dag_id
                if len(dag_name) > 32:
                    dag_name = dag_name[:29] + "..."
                
                avg_str = f"{avg_drift/60:.1f}m" if avg_drift < 3600 else f"{avg_drift/3600:.1f}h"
                p95_str = f"{p95_drift/60:.1f}m" if p95_drift < 3600 else f"{p95_drift/3600:.1f}h"
                
                print(f"{dag_name:<35} {avg_str:<12} {p95_str:<12} {delayed_5min:<10}")
                
                sla_analysis["schedule_drift"].append({
                    "dag_id": dag_id,
                    "avg_drift_seconds": avg_drift,
                    "p95_drift_seconds": p95_drift,
                    "runs_delayed_5min": delayed_5min
                })
                
                if avg_drift > 300:
                    significant_drift.append(dag_id)
            
            if significant_drift:
                sla_analysis["insights"].append(
                    f"⚠️ {len(significant_drift)} DAGs with >5min average schedule drift"
                )
        else:
            print("✅ No significant schedule drift detected")
            
    except Exception as e:
        print(f"Error analyzing schedule drift: {e}")
        sla_analysis["schedule_drift"] = {"error": str(e)}

    # 4. Catchup Backlog
    print("\n📥 CATCHUP BACKLOG")
    print("-" * 40)
    
    try:
        backlog_query = DiagnosticQueries.get_catchup_backlog_query()
        backlog_records = postgres_hook.get_records(backlog_query)
        
        if backlog_records:
            print(f"{'DAG ID':<35} {'Queued':<10} {'Span (h)':<12} {'Age (h)':<12}")
            print("-" * 75)
            
            for record in backlog_records[:15]:  # Top 15
                dag_id = record[0]
                queued_runs = int(record[1])
                span_hours = float(record[4]) if record[4] else 0
                age_hours = float(record[5]) if record[5] else 0
                
                dag_name = dag_id
                if len(dag_name) > 32:
                    dag_name = dag_name[:29] + "..."
                
                print(f"{dag_name:<35} {queued_runs:<10} {span_hours:<12.1f} {age_hours:<12.1f}")
                
                sla_analysis["catchup_backlog"].append({
                    "dag_id": dag_id,
                    "queued_runs": queued_runs,
                    "backlog_span_hours": span_hours,
                    "oldest_run_age_hours": age_hours
                })
            
            total_queued = sum(int(r[1]) for r in backlog_records)
            sla_analysis["insights"].append(
                f"🚨 {len(backlog_records)} DAGs with catchup backlog ({total_queued} queued runs)"
            )
        else:
            print("✅ No catchup backlog detected")
            
    except Exception as e:
        print(f"Error analyzing catchup backlog: {e}")
        sla_analysis["catchup_backlog"] = {"error": str(e)}

    # 5. Scheduling Delay (Execution Date vs Start Date)
    print("\n⏱️ SCHEDULING DELAY")
    print("-" * 40)
    
    try:
        delay_query = DiagnosticQueries.get_scheduling_delay_query(ANALYSIS_DAYS)
        delay_records = postgres_hook.get_records(delay_query)
        
        if delay_records:
            print(f"{'DAG ID':<35} {'Avg Lag':<12} {'P95 Lag':<12} {'% >5min':<10}")
            print("-" * 75)
            
            high_delay_dags = []
            
            for record in delay_records[:15]:  # Top 15
                dag_id = record[0]
                avg_lag = float(record[2]) if record[2] else 0
                p95_lag = float(record[4]) if record[4] else 0
                pct_delayed = float(record[8]) if record[8] else 0
                
                dag_name = dag_id
                if len(dag_name) > 32:
                    dag_name = dag_name[:29] + "..."
                
                avg_str = f"{avg_lag/60:.1f}m" if avg_lag < 3600 else f"{avg_lag/3600:.1f}h"
                p95_str = f"{p95_lag/60:.1f}m" if p95_lag < 3600 else f"{p95_lag/3600:.1f}h"
                
                print(f"{dag_name:<35} {avg_str:<12} {p95_str:<12} {pct_delayed:<10.1f}")
                
                sla_analysis["scheduling_delay"].append({
                    "dag_id": dag_id,
                    "avg_lag_seconds": avg_lag,
                    "p95_lag_seconds": p95_lag,
                    "pct_delayed_5min": pct_delayed
                })
                
                if p95_lag > 600:
                    high_delay_dags.append(dag_id)
            
            if high_delay_dags:
                sla_analysis["insights"].append(
                    f"⚠️ {len(high_delay_dags)} DAGs with P95 scheduling delay >10min"
                )
        else:
            print("✅ No significant scheduling delays detected")
            
    except Exception as e:
        print(f"Error analyzing scheduling delay: {e}")
        sla_analysis["scheduling_delay"] = {"error": str(e)}

    # Summary
    print(f"\n📊 SLA & TIMING ANALYSIS SUMMARY")
    print("-" * 40)
    print(f"Insights Generated: {len(sla_analysis['insights'])}")
    if sla_analysis["insights"]:
        print("\nKey Insights:")
        for insight in sla_analysis["insights"]:
            print(f"  {insight}")
    else:
        print("✅ No significant SLA or timing issues detected")

    return sla_analysis


@task()
def analyze_task_execution_patterns(**context):
    """Analyze task execution patterns including duration trends, retries, queue depth, and zombies"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("TASK EXECUTION PATTERN ANALYSIS")
    print("=" * 80)

    task_analysis = {
        "duration_trends": [],
        "retry_patterns": [],
        "queue_depth": {},
        "executor_lag": [],
        "pool_utilization": [],
        "zombie_tasks": [],
        "insights": []
    }

    # 1. Task Duration Trends
    print("\n⏱️ TASK DURATION TRENDS")
    print("-" * 40)
    
    try:
        duration_query = DiagnosticQueries.get_task_duration_trends_query(ANALYSIS_DAYS)
        duration_records = postgres_hook.get_records(duration_query)
        
        if duration_records:
            print(f"{'DAG.Task':<45} {'Avg (s)':<10} {'Change %':<12} {'Status':<15}")
            print("-" * 85)
            
            degrading_tasks = []
            improving_tasks = []
            
            for record in duration_records[:15]:  # Top 15
                dag_id = record[0]
                task_id = record[1]
                avg_duration = float(record[3]) if record[3] else 0
                change_pct = float(record[11]) if record[11] else 0
                
                task_name = f"{dag_id}.{task_id}"
                if len(task_name) > 42:
                    task_name = task_name[:39] + "..."
                
                # Determine status
                if change_pct > 50:
                    status = "🚨 Degrading"
                    degrading_tasks.append((task_name, change_pct))
                elif change_pct > 20:
                    status = "⚠️ Slower"
                    degrading_tasks.append((task_name, change_pct))
                elif change_pct < -20:
                    status = "✅ Improved"
                    improving_tasks.append((task_name, abs(change_pct)))
                else:
                    status = "➡️ Stable"
                
                print(f"{task_name:<45} {avg_duration:<10.1f} {change_pct:>+11.1f}% {status:<15}")
                
                task_analysis["duration_trends"].append({
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "avg_duration_seconds": avg_duration,
                    "duration_change_percent": change_pct,
                    "execution_count": int(record[2])
                })
            
            if degrading_tasks:
                task_analysis["insights"].append(
                    f"🚨 {len(degrading_tasks)} tasks showing performance degradation (>20% slower)"
                )
            
            if improving_tasks:
                task_analysis["insights"].append(
                    f"✅ {len(improving_tasks)} tasks showing performance improvement"
                )
                
    except Exception as e:
        print(f"Error analyzing duration trends: {e}")
        task_analysis["duration_trends"] = {"error": str(e)}

    # 2. Task Retry Patterns
    print("\n🔄 TASK RETRY PATTERNS")
    print("-" * 40)
    
    try:
        retry_query = DiagnosticQueries.get_task_retry_patterns_query(ANALYSIS_DAYS)
        retry_records = postgres_hook.get_records(retry_query)
        
        if retry_records:
            print(f"{'DAG.Task':<45} {'Retries':<10} {'Retry %':<10} {'Fail %':<10}")
            print("-" * 80)
            
            high_retry_tasks = []
            
            for record in retry_records[:15]:  # Top 15
                dag_id = record[0]
                task_id = record[1]
                retry_attempts = int(record[4])
                retry_rate = float(record[8]) if record[8] else 0
                failure_rate = float(record[9]) if record[9] else 0
                
                task_name = f"{dag_id}.{task_id}"
                if len(task_name) > 42:
                    task_name = task_name[:39] + "..."
                
                print(f"{task_name:<45} {retry_attempts:<10} {retry_rate:<10.1f} {failure_rate:<10.1f}")
                
                task_analysis["retry_patterns"].append({
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "retry_attempts": retry_attempts,
                    "retry_rate_percent": retry_rate,
                    "failure_rate_percent": failure_rate
                })
                
                if retry_rate > 50:
                    high_retry_tasks.append(task_name)
            
            if high_retry_tasks:
                task_analysis["insights"].append(
                    f"⚠️ {len(high_retry_tasks)} tasks with >50% retry rate - investigate transient failures"
                )
                
    except Exception as e:
        print(f"Error analyzing retry patterns: {e}")
        task_analysis["retry_patterns"] = {"error": str(e)}

    # 3. Task Queue Depth Analysis
    print("\n📊 TASK QUEUE DEPTH")
    print("-" * 40)
    
    try:
        queue_query = DiagnosticQueries.get_task_queue_depth_query(ANALYSIS_DAYS)
        queue_records = postgres_hook.get_records(queue_query)
        
        if queue_records and queue_records[0]:
            record = queue_records[0]
            max_queued = int(record[0]) if record[0] else 0
            avg_queued = float(record[1]) if record[1] else 0
            p95_queued = float(record[2]) if record[2] else 0
            max_running = int(record[3]) if record[3] else 0
            avg_running = float(record[4]) if record[4] else 0
            
            print(f"Max Queued Tasks: {max_queued}")
            print(f"Avg Queued Tasks: {avg_queued:.1f}")
            print(f"P95 Queued Tasks: {p95_queued:.1f}")
            print(f"Max Running Tasks: {max_running}")
            print(f"Avg Running Tasks: {avg_running:.1f}")
            
            task_analysis["queue_depth"] = {
                "max_queued": max_queued,
                "avg_queued": avg_queued,
                "p95_queued": p95_queued,
                "max_running": max_running,
                "avg_running": avg_running
            }
            
            if max_queued > 100:
                task_analysis["insights"].append(
                    f"🚨 Peak queue depth of {max_queued} tasks - severe capacity bottleneck"
                )
            elif max_queued > 50:
                task_analysis["insights"].append(
                    f"⚠️ Peak queue depth of {max_queued} tasks - capacity bottleneck detected"
                )
            
            if avg_queued > 20:
                task_analysis["insights"].append(
                    f"⚠️ Average {avg_queued:.0f} tasks queued - increase parallelism or worker capacity"
                )
                
    except Exception as e:
        print(f"Error analyzing queue depth: {e}")
        task_analysis["queue_depth"] = {"error": str(e)}

    # 4. Executor Queue Lag
    print("\n⏳ EXECUTOR QUEUE LAG")
    print("-" * 40)
    
    try:
        lag_query = DiagnosticQueries.get_executor_queue_lag_query(ANALYSIS_DAYS)
        lag_records = postgres_hook.get_records(lag_query)
        
        if lag_records:
            print(f"{'DAG.Task':<45} {'Avg Lag (s)':<12} {'P95 Lag (s)':<12}")
            print("-" * 75)
            
            high_lag_tasks = []
            
            for record in lag_records[:15]:  # Top 15
                dag_id = record[0]
                task_id = record[1]
                avg_lag = float(record[3]) if record[3] else 0
                p95_lag = float(record[6]) if record[6] else 0
                
                task_name = f"{dag_id}.{task_id}"
                if len(task_name) > 42:
                    task_name = task_name[:39] + "..."
                
                print(f"{task_name:<45} {avg_lag:<12.1f} {p95_lag:<12.1f}")
                
                task_analysis["executor_lag"].append({
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "avg_queue_lag_seconds": avg_lag,
                    "p95_queue_lag_seconds": p95_lag
                })
                
                if p95_lag > 60:
                    high_lag_tasks.append(task_name)
            
            if high_lag_tasks:
                task_analysis["insights"].append(
                    f"⚠️ {len(high_lag_tasks)} tasks with P95 queue lag >60s - increase worker capacity"
                )
                
    except Exception as e:
        print(f"Error analyzing executor lag: {e}")
        task_analysis["executor_lag"] = {"error": str(e)}

    # 5. Pool Utilization
    print("\n🏊 POOL UTILIZATION")
    print("-" * 40)
    
    try:
        pool_query = DiagnosticQueries.get_pool_utilization_query(ANALYSIS_DAYS)
        pool_records = postgres_hook.get_records(pool_query)
        
        if pool_records:
            print(f"{'Pool':<25} {'Slots':<8} {'Avg Util %':<12} {'Peak Util %':<12}")
            print("-" * 65)
            
            underutilized_pools = []
            overutilized_pools = []
            
            for record in pool_records:
                pool_name = record[0]
                slots = int(record[1]) if record[1] else 0
                avg_util = float(record[7]) if record[7] else 0
                peak_util = float(record[8]) if record[8] else 0
                
                print(f"{pool_name:<25} {slots:<8} {avg_util:<12.1f} {peak_util:<12.1f}")
                
                task_analysis["pool_utilization"].append({
                    "pool_name": pool_name,
                    "pool_slots": slots,
                    "avg_utilization_percent": avg_util,
                    "peak_utilization_percent": peak_util
                })
                
                if peak_util > 90:
                    overutilized_pools.append(pool_name)
                elif avg_util < 20:
                    underutilized_pools.append(pool_name)
            
            if overutilized_pools:
                task_analysis["insights"].append(
                    f"⚠️ {len(overutilized_pools)} pools exceeding 90% utilization - increase pool slots"
                )
            
            if underutilized_pools:
                task_analysis["insights"].append(
                    f"💡 {len(underutilized_pools)} pools with <20% utilization - consider consolidation"
                )
        else:
            print("No custom pools detected (only default_pool in use)")
            
    except Exception as e:
        print(f"Error analyzing pool utilization: {e}")
        task_analysis["pool_utilization"] = {"error": str(e)}

    # 6. Zombie/Orphaned Task Detection
    print("\n🧟 ZOMBIE TASK DETECTION")
    print("-" * 40)
    
    try:
        zombie_query = DiagnosticQueries.get_zombie_tasks_query()
        zombie_records = postgres_hook.get_records(zombie_query)
        
        if zombie_records:
            print(f"Found {len(zombie_records)} potential zombie tasks")
            print(f"{'DAG.Task':<45} {'Hours Running':<15} {'Hostname':<20}")
            print("-" * 85)
            
            for record in zombie_records[:10]:  # Show top 10
                dag_id = record[0]
                task_id = record[1]
                hours_running = float(record[5]) if record[5] else 0
                hostname = record[6] if record[6] else "Unknown"
                
                task_name = f"{dag_id}.{task_id}"
                if len(task_name) > 42:
                    task_name = task_name[:39] + "..."
                
                print(f"{task_name:<45} {hours_running:<15.1f} {hostname:<20}")
                
                task_analysis["zombie_tasks"].append({
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "hours_running": hours_running,
                    "hostname": hostname
                })
            
            task_analysis["insights"].append(
                f"🚨 {len(zombie_records)} zombie tasks detected (running >6 hours) - investigate and clear"
            )
        else:
            print("✅ No zombie tasks detected")
            
    except Exception as e:
        print(f"Error detecting zombie tasks: {e}")
        task_analysis["zombie_tasks"] = {"error": str(e)}

    # Summary
    print(f"\n📊 TASK EXECUTION ANALYSIS SUMMARY")
    print("-" * 40)
    print(f"Insights Generated: {len(task_analysis['insights'])}")
    if task_analysis["insights"]:
        print("\nKey Insights:")
        for insight in task_analysis["insights"]:
            print(f"  {insight}")
    else:
        print("✅ No significant task execution issues detected")

    return task_analysis


@task()
def analyze_database_performance(**context):
    """Analyze database performance including table sizes, indexes, locks, and growth rate"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("DATABASE PERFORMANCE ANALYSIS")
    print("=" * 80)

    db_analysis = {
        "table_sizes": [],
        "row_counts": [],
        "index_usage": [],
        "lock_contention": [],
        "connection_stats": {},
        "growth_rate": [],
        "insights": []
    }

    # 1. Table Sizes and Bloat Analysis
    print("\n💾 TABLE SIZE ANALYSIS")
    print("-" * 40)
    
    try:
        size_query = DiagnosticQueries.get_database_table_sizes_query()
        size_records = postgres_hook.get_records(size_query)
        
        if size_records:
            print(f"{'Table':<30} {'Total Size':<15} {'Table Size':<15} {'Indexes Size':<15}")
            print("-" * 80)
            
            total_db_size = 0
            large_tables = []
            
            for record in size_records:
                table_name = record[1]
                total_size = record[2]
                total_size_bytes = record[3]
                table_size = record[4]
                indexes_size = record[6]
                
                print(f"{table_name:<30} {total_size:<15} {table_size:<15} {indexes_size:<15}")
                
                db_analysis["table_sizes"].append({
                    "table_name": table_name,
                    "total_size": total_size,
                    "total_size_bytes": total_size_bytes,
                    "table_size": table_size,
                    "indexes_size": indexes_size
                })
                
                total_db_size += total_size_bytes
                
                # Flag large tables (>1GB)
                if total_size_bytes > 1_000_000_000:
                    large_tables.append((table_name, total_size))
            
            print(f"\nTotal Database Size: {total_db_size / (1024**3):.2f} GB")
            
            if large_tables:
                db_analysis["insights"].append(
                    f"⚠️ {len(large_tables)} tables exceed 1GB - consider archiving or partitioning"
                )
                
    except Exception as e:
        print(f"Error analyzing table sizes: {e}")
        db_analysis["table_sizes"] = {"error": str(e)}

    # 2. Row Count Analysis
    print("\n📊 ROW COUNT ANALYSIS")
    print("-" * 40)
    
    try:
        row_query = DiagnosticQueries.get_database_table_row_counts_query()
        row_records = postgres_hook.get_records(row_query)
        
        if row_records:
            print(f"{'Table':<20} {'Total Rows':<15} {'Running':<12} {'Queued':<12} {'Failed':<12}")
            print("-" * 75)
            
            for record in row_records:
                table_name = record[0]
                row_count = record[1]
                running = record[2] if record[2] is not None else 0
                queued = record[3] if record[3] is not None else 0
                failed = record[4] if record[4] is not None else 0
                
                print(f"{table_name:<20} {row_count:<15,} {running:<12,} {queued:<12,} {failed:<12,}")
                
                db_analysis["row_counts"].append({
                    "table_name": table_name,
                    "row_count": row_count,
                    "running_count": running,
                    "queued_count": queued,
                    "failed_count": failed
                })
                
                # Generate insights
                if table_name == "task_instance" and row_count > 10_000_000:
                    db_analysis["insights"].append(
                        f"🚨 task_instance table has {row_count:,} rows - urgent cleanup needed"
                    )
                elif table_name == "task_instance" and row_count > 5_000_000:
                    db_analysis["insights"].append(
                        f"⚠️ task_instance table has {row_count:,} rows - cleanup recommended"
                    )
                
                if table_name == "log" and row_count > 1_000_000:
                    db_analysis["insights"].append(
                        f"⚠️ log table has {row_count:,} rows - consider log rotation"
                    )
                
                if table_name == "xcom" and row_count > 1_000_000:
                    db_analysis["insights"].append(
                        f"⚠️ xcom table has {row_count:,} rows - review XCom usage patterns"
                    )
                    
    except Exception as e:
        print(f"Error analyzing row counts: {e}")
        db_analysis["row_counts"] = {"error": str(e)}

    # 3. Index Usage Analysis
    print("\n🔍 INDEX USAGE ANALYSIS")
    print("-" * 40)
    
    try:
        index_query = DiagnosticQueries.get_database_index_usage_query()
        index_records = postgres_hook.get_records(index_query)
        
        if index_records:
            print(f"{'Table':<25} {'Index':<35} {'Scans':<12} {'Size':<12}")
            print("-" * 90)
            
            unused_indexes = []
            large_unused_indexes = []
            
            for record in index_records[:15]:  # Show top 15
                table_name = record[1]
                index_name = record[2]
                scans = record[3]
                index_size = record[6]
                index_size_bytes = record[7]
                
                print(f"{table_name:<25} {index_name:<35} {scans:<12,} {index_size:<12}")
                
                db_analysis["index_usage"].append({
                    "table_name": table_name,
                    "index_name": index_name,
                    "scans": scans,
                    "index_size": index_size,
                    "index_size_bytes": index_size_bytes
                })
                
                # Flag unused indexes
                if scans == 0:
                    unused_indexes.append(index_name)
                    if index_size_bytes > 10_000_000:  # >10MB
                        large_unused_indexes.append((index_name, index_size))
            
            if unused_indexes:
                print(f"\n⚠️ Found {len(unused_indexes)} unused indexes")
                if large_unused_indexes:
                    db_analysis["insights"].append(
                        f"💡 {len(large_unused_indexes)} large unused indexes consuming space - consider dropping"
                    )
                    
    except Exception as e:
        print(f"Error analyzing indexes: {e}")
        db_analysis["index_usage"] = {"error": str(e)}

    # 4. Lock Contention Analysis
    print("\n🔒 LOCK CONTENTION ANALYSIS")
    print("-" * 40)
    
    try:
        lock_query = DiagnosticQueries.get_database_lock_contention_query()
        lock_records = postgres_hook.get_records(lock_query)
        
        if lock_records:
            print(f"Found {len(lock_records)} blocked queries")
            print(f"{'PID':<8} {'User':<15} {'State':<12} {'Wait Event':<20} {'Duration':<15}")
            print("-" * 75)
            
            for record in lock_records[:10]:  # Show top 10
                pid = record[0]
                user = record[1]
                state = record[3]
                wait_event = record[6] if record[6] else "N/A"
                duration = str(record[7])
                
                print(f"{pid:<8} {user:<15} {state:<12} {wait_event:<20} {duration:<15}")
                
                db_analysis["lock_contention"].append({
                    "pid": pid,
                    "user": user,
                    "state": state,
                    "wait_event": wait_event,
                    "duration": str(duration)
                })
            
            if len(lock_records) > 5:
                db_analysis["insights"].append(
                    f"🚨 {len(lock_records)} queries blocked by locks - investigate lock contention"
                )
            elif len(lock_records) > 0:
                db_analysis["insights"].append(
                    f"⚠️ {len(lock_records)} queries blocked by locks - monitor for patterns"
                )
        else:
            print("✅ No lock contention detected")
            
    except Exception as e:
        print(f"Error analyzing locks: {e}")
        db_analysis["lock_contention"] = {"error": str(e)}

    # 5. Connection Statistics
    print("\n🔌 CONNECTION STATISTICS")
    print("-" * 40)
    
    try:
        conn_query = DiagnosticQueries.get_database_connection_stats_query()
        conn_records = postgres_hook.get_records(conn_query)
        
        if conn_records and conn_records[0]:
            record = conn_records[0]
            total_conn = record[0]
            active_conn = record[1]
            idle_conn = record[2]
            idle_in_tx = record[3]
            waiting_conn = record[4]
            max_age = record[5]
            avg_age = record[6]
            
            print(f"Total Connections: {total_conn}")
            print(f"Active: {active_conn}")
            print(f"Idle: {idle_conn}")
            print(f"Idle in Transaction: {idle_in_tx}")
            print(f"Waiting: {waiting_conn}")
            print(f"Max Connection Age: {max_age:.0f}s")
            print(f"Avg Connection Age: {avg_age:.0f}s")
            
            db_analysis["connection_stats"] = {
                "total_connections": total_conn,
                "active_connections": active_conn,
                "idle_connections": idle_conn,
                "idle_in_transaction": idle_in_tx,
                "waiting_connections": waiting_conn,
                "max_connection_age_seconds": max_age,
                "avg_connection_age_seconds": avg_age
            }
            
            # Check against pool size
            try:
                pool_size = int(conf.get("database", "sql_alchemy_pool_size", fallback="5"))
                max_overflow = int(conf.get("database", "sql_alchemy_max_overflow", fallback="10"))
                max_pool = pool_size + max_overflow
                
                utilization = (total_conn / max_pool) * 100 if max_pool > 0 else 0
                print(f"\nPool Utilization: {utilization:.1f}% ({total_conn}/{max_pool})")
                
                if utilization > 90:
                    db_analysis["insights"].append(
                        f"🚨 Connection pool {utilization:.0f}% utilized - risk of exhaustion"
                    )
                elif utilization > 75:
                    db_analysis["insights"].append(
                        f"⚠️ Connection pool {utilization:.0f}% utilized - monitor closely"
                    )
            except Exception:
                pass
            
            if idle_in_tx > 5:
                db_analysis["insights"].append(
                    f"⚠️ {idle_in_tx} idle-in-transaction connections - may indicate transaction management issues"
                )
                
    except Exception as e:
        print(f"Error analyzing connections: {e}")
        db_analysis["connection_stats"] = {"error": str(e)}

    # 6. Growth Rate Analysis
    print("\n📈 DATABASE GROWTH RATE")
    print("-" * 40)
    
    try:
        growth_query = DiagnosticQueries.get_database_growth_rate_query(ANALYSIS_DAYS)
        growth_records = postgres_hook.get_records(growth_query)
        
        if growth_records:
            print(f"{'Table':<20} {'Total Rows':<15} {'Recent Rows':<15} {'Rows/Day':<12} {'Recent %':<12}")
            print("-" * 80)
            
            for record in growth_records:
                table_name = record[0]
                current_rows = record[1]
                recent_rows = record[2]
                rows_per_day = float(record[3]) if record[3] else 0
                recent_pct = float(record[4]) if record[4] else 0
                
                print(f"{table_name:<20} {current_rows:<15,} {recent_rows:<15,} {rows_per_day:<12,.0f} {recent_pct:<12.1f}%")
                
                db_analysis["growth_rate"].append({
                    "table_name": table_name,
                    "current_rows": current_rows,
                    "recent_rows": recent_rows,
                    "rows_per_day": rows_per_day,
                    "recent_percentage": recent_pct
                })
                
                # Project when cleanup will be needed
                if table_name == "task_instance" and rows_per_day > 10000:
                    days_to_10m = (10_000_000 - current_rows) / rows_per_day if rows_per_day > 0 else 999
                    if days_to_10m < 30:
                        db_analysis["insights"].append(
                            f"⚠️ task_instance growing at {rows_per_day:,.0f} rows/day - will reach 10M in {days_to_10m:.0f} days"
                        )
                        
    except Exception as e:
        print(f"Error analyzing growth rate: {e}")
        db_analysis["growth_rate"] = {"error": str(e)}

    # Summary
    print(f"\n📊 DATABASE ANALYSIS SUMMARY")
    print("-" * 40)
    print(f"Insights Generated: {len(db_analysis['insights'])}")
    if db_analysis["insights"]:
        print("\nKey Insights:")
        for insight in db_analysis["insights"]:
            print(f"  {insight}")
    else:
        print("✅ No significant database issues detected")

    return db_analysis


@task()
def analyze_traffic_patterns(**context):
    """Analyze DAG execution traffic patterns to identify scheduling hotspots and resource utilization"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("TRAFFIC PATTERN ANALYSIS")
    print("=" * 80)

    traffic_analysis = {
        "hourly_patterns": {},
        "concurrent_patterns": {},
        "dag_frequency": [],
        "task_concurrency": {},
        "insights": []
    }

    # 1. Hourly Traffic Patterns
    print("\n📊 HOURLY TRAFFIC PATTERNS")
    print("-" * 40)
    
    try:
        hourly_query = DiagnosticQueries.get_traffic_pattern_hourly_query(ANALYSIS_DAYS)
        hourly_records = postgres_hook.get_records(hourly_query)
        
        if hourly_records:
            print(f"{'Hour':<6} {'Total Runs':<12} {'Avg/Day':<10} {'Max/Hour':<10} {'Avg Duration':<15}")
            print("-" * 60)
            
            hourly_data = []
            peak_hour = None
            peak_runs = 0
            quiet_hours = []
            
            for record in hourly_records:
                hour = int(record[0])
                total_runs = int(record[1])
                avg_runs = float(record[2]) if record[2] else 0
                max_runs = int(record[3]) if record[3] else 0
                avg_duration = float(record[5]) if record[5] else 0
                
                duration_str = f"{avg_duration/60:.1f}m" if avg_duration < 3600 else f"{avg_duration/3600:.1f}h"
                
                print(f"{hour:02d}:00  {total_runs:<12} {avg_runs:<10.1f} {max_runs:<10} {duration_str:<15}")
                
                hourly_data.append({
                    "hour": hour,
                    "total_runs": total_runs,
                    "avg_runs_per_day": avg_runs,
                    "max_runs_in_hour": max_runs,
                    "avg_duration_seconds": avg_duration
                })
                
                # Track peak hour
                if total_runs > peak_runs:
                    peak_runs = total_runs
                    peak_hour = hour
                
                # Track quiet hours (less than 10% of peak)
                if total_runs < peak_runs * 0.1 and total_runs > 0:
                    quiet_hours.append(hour)
            
            traffic_analysis["hourly_patterns"] = {
                "data": hourly_data,
                "peak_hour": peak_hour,
                "peak_runs": peak_runs,
                "quiet_hours": quiet_hours
            }
            
            print(f"\n🔥 Peak Hour: {peak_hour:02d}:00 with {peak_runs} total runs")
            if quiet_hours:
                quiet_hours_str = ", ".join([f"{h:02d}:00" for h in quiet_hours[:5]])
                print(f"😴 Quiet Hours: {quiet_hours_str}")
                if len(quiet_hours) > 5:
                    print(f"   ... and {len(quiet_hours) - 5} more")
            
            # Generate insights
            if peak_runs > 100:
                traffic_analysis["insights"].append(
                    f"⚠️ High traffic during peak hour ({peak_hour:02d}:00) with {peak_runs} runs - consider load balancing"
                )
            
            if len(quiet_hours) > 8:
                traffic_analysis["insights"].append(
                    f"💡 {len(quiet_hours)} quiet hours detected - opportunity to shift non-critical workloads"
                )
                
    except Exception as e:
        print(f"Error analyzing hourly patterns: {e}")
        traffic_analysis["hourly_patterns"]["error"] = str(e)

    # 2. Concurrent DAG Run Patterns
    print("\n🔄 CONCURRENT DAG RUN PATTERNS")
    print("-" * 40)
    
    try:
        concurrent_query = DiagnosticQueries.get_traffic_pattern_concurrent_query(ANALYSIS_DAYS)
        concurrent_records = postgres_hook.get_records(concurrent_query)
        
        if concurrent_records and concurrent_records[0]:
            record = concurrent_records[0]
            max_concurrent = int(record[0]) if record[0] else 0
            avg_concurrent = float(record[1]) if record[1] else 0
            p95_concurrent = float(record[2]) if record[2] else 0
            p99_concurrent = float(record[3]) if record[3] else 0
            
            print(f"Max Concurrent DAGs: {max_concurrent}")
            print(f"Avg Concurrent DAGs: {avg_concurrent:.1f}")
            print(f"P95 Concurrent DAGs: {p95_concurrent:.1f}")
            print(f"P99 Concurrent DAGs: {p99_concurrent:.1f}")
            
            traffic_analysis["concurrent_patterns"] = {
                "max_concurrent_dags": max_concurrent,
                "avg_concurrent_dags": avg_concurrent,
                "p95_concurrent_dags": p95_concurrent,
                "p99_concurrent_dags": p99_concurrent
            }
            
            # Check against parallelism settings
            try:
                parallelism = int(conf.get("core", "parallelism", fallback="32"))
                max_active_runs = int(conf.get("core", "max_active_runs_per_dag", fallback="16"))
                
                if max_concurrent > max_active_runs * 0.8:
                    traffic_analysis["insights"].append(
                        f"⚠️ Peak concurrent DAGs ({max_concurrent}) approaching max_active_runs_per_dag ({max_active_runs})"
                    )
                
                if p95_concurrent > max_active_runs * 0.6:
                    traffic_analysis["insights"].append(
                        f"💡 P95 concurrent DAGs ({p95_concurrent:.0f}) is high - consider increasing max_active_runs_per_dag"
                    )
            except Exception:
                pass
                
    except Exception as e:
        print(f"Error analyzing concurrent patterns: {e}")
        traffic_analysis["concurrent_patterns"]["error"] = str(e)

    # 3. DAG Frequency Analysis
    print("\n📈 DAG EXECUTION FREQUENCY")
    print("-" * 40)
    
    try:
        frequency_query = DiagnosticQueries.get_traffic_pattern_dag_frequency_query(ANALYSIS_DAYS)
        frequency_records = postgres_hook.get_records(frequency_query)
        
        if frequency_records:
            print(f"{'DAG ID':<35} {'Total Runs':<12} {'Runs/Day':<10} {'Variance':<15}")
            print("-" * 75)
            
            high_frequency_dags = []
            variable_duration_dags = []
            
            for record in frequency_records[:15]:  # Top 15
                dag_id = record[0]
                total_runs = int(record[1])
                runs_per_day = float(record[3]) if record[3] else 0
                variance = record[6] if record[6] else "Unknown"
                
                print(f"{dag_id:<35} {total_runs:<12} {runs_per_day:<10.1f} {variance:<15}")
                
                traffic_analysis["dag_frequency"].append({
                    "dag_id": dag_id,
                    "total_runs": total_runs,
                    "runs_per_day": runs_per_day,
                    "duration_variance": variance
                })
                
                # Track high frequency DAGs
                if runs_per_day > 50:
                    high_frequency_dags.append((dag_id, runs_per_day))
                
                # Track variable duration DAGs
                if variance == "High Variance":
                    variable_duration_dags.append(dag_id)
            
            if high_frequency_dags:
                print(f"\n🔥 High Frequency DAGs ({len(high_frequency_dags)}):")
                for dag_id, rpd in high_frequency_dags[:3]:
                    print(f"   - {dag_id}: {rpd:.1f} runs/day")
                traffic_analysis["insights"].append(
                    f"⚠️ {len(high_frequency_dags)} DAGs running >50 times/day - verify scheduling is intentional"
                )
            
            if variable_duration_dags:
                print(f"\n⚠️ Variable Duration DAGs ({len(variable_duration_dags)}):")
                for dag_id in variable_duration_dags[:3]:
                    print(f"   - {dag_id}")
                traffic_analysis["insights"].append(
                    f"💡 {len(variable_duration_dags)} DAGs have high duration variance - investigate performance inconsistencies"
                )
                
    except Exception as e:
        print(f"Error analyzing DAG frequency: {e}")
        traffic_analysis["dag_frequency"] = {"error": str(e)}

    # 4. Task Concurrency Patterns
    print("\n⚙️ TASK CONCURRENCY PATTERNS")
    print("-" * 40)
    
    try:
        task_query = DiagnosticQueries.get_traffic_pattern_task_concurrency_query(ANALYSIS_DAYS)
        task_records = postgres_hook.get_records(task_query)
        
        if task_records and task_records[0]:
            record = task_records[0]
            max_concurrent_tasks = int(record[0]) if record[0] else 0
            avg_concurrent_tasks = float(record[1]) if record[1] else 0
            p95_concurrent_tasks = float(record[2]) if record[2] else 0
            p99_concurrent_tasks = float(record[3]) if record[3] else 0
            
            print(f"Max Concurrent Tasks: {max_concurrent_tasks}")
            print(f"Avg Concurrent Tasks: {avg_concurrent_tasks:.1f}")
            print(f"P95 Concurrent Tasks: {p95_concurrent_tasks:.1f}")
            print(f"P99 Concurrent Tasks: {p99_concurrent_tasks:.1f}")
            
            traffic_analysis["task_concurrency"] = {
                "max_concurrent_tasks": max_concurrent_tasks,
                "avg_concurrent_tasks": avg_concurrent_tasks,
                "p95_concurrent_tasks": p95_concurrent_tasks,
                "p99_concurrent_tasks": p99_concurrent_tasks
            }
            
            # Check against parallelism
            try:
                parallelism = int(conf.get("core", "parallelism", fallback="32"))
                
                utilization = (avg_concurrent_tasks / parallelism) * 100
                peak_utilization = (max_concurrent_tasks / parallelism) * 100
                
                print(f"\nParallelism Utilization:")
                print(f"  Average: {utilization:.1f}%")
                print(f"  Peak: {peak_utilization:.1f}%")
                
                if peak_utilization > 90:
                    traffic_analysis["insights"].append(
                        f"🚨 Peak task concurrency ({max_concurrent_tasks}) exceeds 90% of parallelism ({parallelism}) - increase parallelism"
                    )
                elif peak_utilization > 75:
                    traffic_analysis["insights"].append(
                        f"⚠️ Peak task concurrency ({max_concurrent_tasks}) exceeds 75% of parallelism ({parallelism}) - monitor closely"
                    )
                elif utilization < 30:
                    traffic_analysis["insights"].append(
                        f"💡 Low average utilization ({utilization:.1f}%) - parallelism ({parallelism}) may be over-provisioned"
                    )
            except Exception:
                pass
                
    except Exception as e:
        print(f"Error analyzing task concurrency: {e}")
        traffic_analysis["task_concurrency"]["error"] = str(e)

    # Summary
    print(f"\n📊 TRAFFIC ANALYSIS SUMMARY")
    print("-" * 40)
    print(f"Insights Generated: {len(traffic_analysis['insights'])}")
    if traffic_analysis["insights"]:
        print("\nKey Insights:")
        for insight in traffic_analysis["insights"]:
            print(f"  {insight}")
    else:
        print("✅ No significant traffic pattern issues detected")

    return traffic_analysis


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
    error_pattern_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_error_patterns"
    )
    scheduler_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_scheduler_performance"
    )
    predictive_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_predictive_patterns"
    )
    sla_timing_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_sla_and_timing"
    )
    task_execution_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_task_execution_patterns"
    )
    database_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_database_performance"
    )
    traffic_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_traffic_patterns"
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
    
    # Configuration conflicts and issues (PROMINENT SECTION)
    config_issues = config_analysis.get("issues", [])
    if config_issues:
        report_lines.append(f"\n🚨 CONFIGURATION CONFLICTS AND ISSUES ({len(config_issues)}):")
        report_lines.append("-" * 50)
        for issue in config_issues:
            # Add indentation for readability
            report_lines.append(f"{issue}")
        report_lines.append("")
    else:
        report_lines.append("\n✅ No configuration conflicts detected")
        report_lines.append("")

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
                "min_file_process_interval",
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
            "database": [
                "sql_alchemy_pool_size",
                "sql_alchemy_max_overflow",
                "sql_alchemy_pool_recycle",
                "sql_alchemy_pool_pre_ping",
            ],
        }

        # Impact levels for display (only for settings included in report)
        impact_levels = {
            "executor": "HIGH",
            "parallelism": "HIGH",
            "max_active_tasks_per_dag": "HIGH",
            "max_active_runs_per_dag": "HIGH",
            "dagbag_import_timeout": "HIGH",
            "dag_file_processor_timeout": "HIGH",
            "min_serialized_dag_update_interval": "HIGH",
            "min_serialized_dag_fetch_interval": "HIGH",
            "dag_dir_list_interval": "HIGH",
            "min_file_process_interval": "HIGH",
            "parsing_processes": "HIGH",
            "worker_concurrency": "HIGH",
            "sql_alchemy_pool_size": "HIGH",
            "sql_alchemy_max_overflow": "MEDIUM",
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

    # Error Pattern Analysis Section
    report_lines.append("🔍 ERROR PATTERN ANALYSIS")
    report_lines.append("-" * 50)
    
    try:
        if error_pattern_analysis and isinstance(error_pattern_analysis, dict):
            # Error clusters
            clusters = error_pattern_analysis.get("error_clusters", [])
            if clusters and isinstance(clusters, list):
                high_freq = [c for c in clusters if isinstance(c, dict) and c.get("frequency_category") in ["Very High", "High"]]
                if high_freq:
                    report_lines.append(f"Error Clusters: {len(high_freq)} high-frequency error patterns")
            
            # Correlations
            correlations = error_pattern_analysis.get("error_correlations", [])
            if correlations and isinstance(correlations, list) and len(correlations) > 0:
                report_lines.append(f"Error Correlations: {len(correlations)} pairs of errors occurring together")
            
            # Transient vs persistent
            transient = error_pattern_analysis.get("transient_vs_persistent", [])
            if transient and isinstance(transient, list):
                transient_count = len([t for t in transient if isinstance(t, dict) and t.get("failure_type") == "Transient"])
                persistent_count = len([t for t in transient if isinstance(t, dict) and t.get("failure_type") == "Persistent"])
                report_lines.append(f"\nFailure Types:")
                report_lines.append(f"  Transient (retryable): {transient_count}")
                report_lines.append(f"  Persistent (need fixes): {persistent_count}")
            
            # Time patterns
            time_patterns = error_pattern_analysis.get("time_patterns", [])
            if time_patterns and isinstance(time_patterns, list):
                high_hours = [t.get("hour_of_day") for t in time_patterns if isinstance(t, dict) and t.get("error_intensity") == "High"]
                if high_hours:
                    hours_str = ", ".join([f"{h:02d}:00" for h in high_hours[:5]])
                    report_lines.append(f"\nError Spikes: {hours_str}")
            
            # Cascades
            cascades = error_pattern_analysis.get("failure_cascades", [])
            if cascades and isinstance(cascades, list) and len(cascades) > 0:
                total_cascades = sum(c.get("cascade_count", 0) for c in cascades if isinstance(c, dict))
                report_lines.append(f"\nFailure Cascades: {len(cascades)} DAGs ({total_cascades} total cascades)")
            
            # Error insights
            error_insights = error_pattern_analysis.get("insights", [])
            if error_insights and isinstance(error_insights, list):
                report_lines.append(f"\nError Pattern Insights:")
                for insight in error_insights:
                    report_lines.append(f"  {insight}")
        else:
            report_lines.append("Error pattern analysis not available")
    except Exception as e:
        report_lines.append(f"Error displaying error pattern analysis: {str(e)}")
    
    report_lines.append("")

    # Scheduler Performance Section
    report_lines.append("⚙️ SCHEDULER PERFORMANCE")
    report_lines.append("-" * 50)
    
    try:
        if scheduler_analysis and isinstance(scheduler_analysis, dict):
            # Heartbeat
            heartbeat = scheduler_analysis.get("heartbeat", [])
            if heartbeat and isinstance(heartbeat, list):
                max_gap = max((h.get("max_gap_seconds", 0) for h in heartbeat if isinstance(h, dict)), default=0)
                if max_gap > 60:
                    report_lines.append(f"Heartbeat: Max gap {max_gap:.0f}s detected")
                else:
                    report_lines.append(f"Heartbeat: Healthy")
            
            # Parsing times
            parsing = scheduler_analysis.get("parsing_times", [])
            if parsing and isinstance(parsing, list):
                slow = [p for p in parsing if isinstance(p, dict) and p.get("avg_duration_seconds", 0) > 30]
                if slow:
                    report_lines.append(f"DAG Parsing: {len(slow)} slow files (>30s)")
            
            # Loop duration
            loop_dur = scheduler_analysis.get("loop_duration", [])
            if loop_dur and isinstance(loop_dur, list):
                for loop in loop_dur:
                    if isinstance(loop, dict):
                        p95 = loop.get("p95_duration_seconds", 0)
                        if p95 > 10:
                            report_lines.append(f"Loop Duration: P95 {p95:.1f}s - performance issue")
            
            # Queue duration
            queue_dur = scheduler_analysis.get("queue_duration", [])
            if queue_dur and isinstance(queue_dur, list):
                long_queue = [q for q in queue_dur if isinstance(q, dict) and q.get("p95_queue_seconds", 0) > 300]
                if long_queue:
                    report_lines.append(f"Queue Duration: {len(long_queue)} tasks wait >5min")
            
            # Scheduler insights
            sched_insights = scheduler_analysis.get("insights", [])
            if sched_insights and isinstance(sched_insights, list):
                report_lines.append(f"\nScheduler Insights:")
                for insight in sched_insights:
                    report_lines.append(f"  {insight}")
        else:
            report_lines.append("Scheduler analysis not available")
    except Exception as e:
        report_lines.append(f"Error displaying scheduler analysis: {str(e)}")
    
    report_lines.append("")

    # Predictive Analytics Section
    report_lines.append("🔮 PREDICTIVE ANALYTICS")
    report_lines.append("-" * 50)
    
    try:
        if predictive_analysis and isinstance(predictive_analysis, dict):
            # Failure predictions
            failure_preds = predictive_analysis.get("failure_predictions", [])
            if failure_preds and isinstance(failure_preds, list):
                high_risk = [t for t in failure_preds if isinstance(t, dict) and ("Critical" in str(t.get("risk_level", "")) or "High" in str(t.get("risk_level", "")))]
                increasing = [t for t in failure_preds if isinstance(t, dict) and t.get("trend") == "Increasing"]
                if high_risk:
                    report_lines.append(f"High-Risk Tasks: {len(high_risk)} tasks likely to fail")
                if increasing:
                    report_lines.append(f"Increasing Failures: {len(increasing)} tasks with worsening trends")
            
            # Capacity forecast
            capacity = predictive_analysis.get("capacity_forecast", {})
            if capacity and isinstance(capacity, dict):
                task_growth = capacity.get("task_growth_per_day", 0)
                if task_growth != 0:
                    report_lines.append(f"\nCapacity Forecast:")
                    report_lines.append(f"  Task Growth: {task_growth:+.2f} tasks/day")
                    if task_growth > 10:
                        report_lines.append(f"  ⚠️ Rapid growth detected")
            
            # Performance degradation
            degradation = predictive_analysis.get("performance_degradation", [])
            if degradation and isinstance(degradation, list):
                high_conf = [t for t in degradation if isinstance(t, dict) and t.get("trend_confidence", 0) > 0.7]
                if high_conf:
                    report_lines.append(f"\nPerformance Degradation: {len(high_conf)} tasks slowing down consistently")
            
            # Anomalies
            anomalies = predictive_analysis.get("anomalies", [])
            if anomalies and isinstance(anomalies, list) and len(anomalies) > 0:
                total_anomalies = sum(a.get("anomaly_count", 0) for a in anomalies if isinstance(a, dict))
                report_lines.append(f"\nAnomalies: {len(anomalies)} tasks with unusual behavior ({total_anomalies} events)")
            
            # Seasonal patterns
            seasonal = predictive_analysis.get("seasonal_patterns", [])
            if seasonal and isinstance(seasonal, list):
                high_var = [s for s in seasonal if isinstance(s, dict) and s.get("coefficient_of_variation", 0) > 50]
                if high_var:
                    days = [s.get("day_name", "") for s in high_var]
                    report_lines.append(f"\nSeasonal Variance: High on {', '.join(days)}")
            
            # Predictive insights
            pred_insights = predictive_analysis.get("insights", [])
            if pred_insights and isinstance(pred_insights, list):
                report_lines.append(f"\nPredictive Insights:")
                for insight in pred_insights:
                    report_lines.append(f"  {insight}")
        else:
            report_lines.append("Predictive analysis not available")
    except Exception as e:
        report_lines.append(f"Error displaying predictive analysis: {str(e)}")
    
    report_lines.append("")

    # SLA & Timing Analysis Section
    report_lines.append("⏰ SLA & TIMING ANALYSIS")
    report_lines.append("-" * 50)
    
    try:
        if sla_timing_analysis and isinstance(sla_timing_analysis, dict):
            # SLA misses
            sla_misses = sla_timing_analysis.get("sla_misses", [])
            if sla_misses and isinstance(sla_misses, list) and len(sla_misses) > 0:
                total_misses = sum(s.get("miss_count", 0) for s in sla_misses if isinstance(s, dict))
                report_lines.append(f"SLA Misses: {len(sla_misses)} tasks ({total_misses} total misses)")
            else:
                report_lines.append(f"SLA Misses: None detected")
            
            # Execution variance
            exec_variance = sla_timing_analysis.get("execution_variance", [])
            if exec_variance and isinstance(exec_variance, list):
                high_var = [t for t in exec_variance if isinstance(t, dict) and t.get("coefficient_of_variation", 0) > 50]
                if high_var:
                    report_lines.append(f"High Variance Tasks: {len(high_var)} tasks with unpredictable duration")
            
            # Schedule drift
            schedule_drift = sla_timing_analysis.get("schedule_drift", [])
            if schedule_drift and isinstance(schedule_drift, list) and len(schedule_drift) > 0:
                avg_drift = sum(s.get("avg_drift_seconds", 0) for s in schedule_drift if isinstance(s, dict)) / len(schedule_drift)
                report_lines.append(f"\nSchedule Drift:")
                report_lines.append(f"  DAGs with drift: {len(schedule_drift)}")
                report_lines.append(f"  Average drift: {avg_drift/60:.1f} minutes")
            
            # Catchup backlog
            catchup = sla_timing_analysis.get("catchup_backlog", [])
            if catchup and isinstance(catchup, list) and len(catchup) > 0:
                total_queued = sum(c.get("queued_runs", 0) for c in catchup if isinstance(c, dict))
                report_lines.append(f"\nCatchup Backlog:")
                report_lines.append(f"  DAGs with backlog: {len(catchup)}")
                report_lines.append(f"  Total queued runs: {total_queued}")
            else:
                report_lines.append(f"\nCatchup Backlog: None")
            
            # Scheduling delay
            sched_delay = sla_timing_analysis.get("scheduling_delay", [])
            if sched_delay and isinstance(sched_delay, list) and len(sched_delay) > 0:
                avg_delay = sum(s.get("avg_lag_seconds", 0) for s in sched_delay if isinstance(s, dict)) / len(sched_delay)
                report_lines.append(f"\nScheduling Delay:")
                report_lines.append(f"  DAGs with delay: {len(sched_delay)}")
                report_lines.append(f"  Average delay: {avg_delay/60:.1f} minutes")
            
            # SLA insights
            sla_insights = sla_timing_analysis.get("insights", [])
            if sla_insights and isinstance(sla_insights, list):
                report_lines.append(f"\nSLA & Timing Insights:")
                for insight in sla_insights:
                    report_lines.append(f"  {insight}")
        else:
            report_lines.append("SLA & timing analysis not available")
    except Exception as e:
        report_lines.append(f"Error displaying SLA analysis: {str(e)}")
    
    report_lines.append("")

    # Task Execution Pattern Analysis Section
    report_lines.append("⚙️ TASK EXECUTION PATTERN ANALYSIS")
    report_lines.append("-" * 50)
    
    try:
        if task_execution_analysis and isinstance(task_execution_analysis, dict):
            # Duration trends
            duration_trends = task_execution_analysis.get("duration_trends", [])
            if duration_trends and isinstance(duration_trends, list):
                degrading = [t for t in duration_trends if isinstance(t, dict) and t.get("duration_change_percent", 0) > 20]
                if degrading:
                    report_lines.append(f"Degrading Tasks: {len(degrading)} tasks showing >20% slowdown")
            
            # Retry patterns
            retry_patterns = task_execution_analysis.get("retry_patterns", [])
            if retry_patterns and isinstance(retry_patterns, list):
                high_retry = [t for t in retry_patterns if isinstance(t, dict) and t.get("retry_rate_percent", 0) > 50]
                if high_retry:
                    report_lines.append(f"High Retry Tasks: {len(high_retry)} tasks with >50% retry rate")
            
            # Queue depth
            queue_depth = task_execution_analysis.get("queue_depth", {})
            if queue_depth and isinstance(queue_depth, dict):
                max_queued = queue_depth.get("max_queued", 0)
                avg_queued = queue_depth.get("avg_queued", 0)
                report_lines.append(f"\nQueue Depth:")
                report_lines.append(f"  Max Queued: {max_queued} tasks")
                report_lines.append(f"  Avg Queued: {avg_queued:.1f} tasks")
            
            # Executor lag
            executor_lag = task_execution_analysis.get("executor_lag", [])
            if executor_lag and isinstance(executor_lag, list):
                high_lag = [t for t in executor_lag if isinstance(t, dict) and t.get("p95_queue_lag_seconds", 0) > 60]
                if high_lag:
                    report_lines.append(f"\nExecutor Lag: {len(high_lag)} tasks with P95 lag >60s")
            
            # Pool utilization
            pool_util = task_execution_analysis.get("pool_utilization", [])
            if pool_util and isinstance(pool_util, list) and len(pool_util) > 0:
                report_lines.append(f"\nPool Utilization:")
                for pool in pool_util[:5]:
                    if isinstance(pool, dict):
                        name = pool.get("pool_name", "unknown")
                        avg_util = pool.get("avg_utilization_percent", 0)
                        report_lines.append(f"  {name}: {avg_util:.1f}% avg")
            
            # Zombie tasks
            zombie_tasks = task_execution_analysis.get("zombie_tasks", [])
            if zombie_tasks and isinstance(zombie_tasks, list) and len(zombie_tasks) > 0:
                report_lines.append(f"\n🚨 Zombie Tasks: {len(zombie_tasks)} tasks stuck in running state")
            else:
                report_lines.append(f"\nZombie Tasks: None detected")
            
            # Task execution insights
            task_insights = task_execution_analysis.get("insights", [])
            if task_insights and isinstance(task_insights, list):
                report_lines.append(f"\nTask Execution Insights:")
                for insight in task_insights:
                    report_lines.append(f"  {insight}")
        else:
            report_lines.append("Task execution analysis not available")
    except Exception as e:
        report_lines.append(f"Error displaying task execution analysis: {str(e)}")
    
    report_lines.append("")

    # Database Performance Analysis Section
    report_lines.append("💾 DATABASE PERFORMANCE ANALYSIS")
    report_lines.append("-" * 50)
    
    try:
        if database_analysis and isinstance(database_analysis, dict):
            # Table sizes
            table_sizes = database_analysis.get("table_sizes", [])
            if table_sizes and isinstance(table_sizes, list) and len(table_sizes) > 0:
                total_size = sum(t.get("total_size_bytes", 0) for t in table_sizes if isinstance(t, dict))
                report_lines.append(f"Total Database Size: {total_size / (1024**3):.2f} GB")
                report_lines.append(f"Largest Tables: {len([t for t in table_sizes if isinstance(t, dict) and t.get('total_size_bytes', 0) > 100_000_000])}")
            
            # Row counts
            row_counts = database_analysis.get("row_counts", [])
            if row_counts and isinstance(row_counts, list):
                report_lines.append(f"\nRow Counts:")
                for table in row_counts:
                    if isinstance(table, dict):
                        name = table.get("table_name", "unknown")
                        count = table.get("row_count", 0)
                        report_lines.append(f"  {name}: {count:,} rows")
            
            # Index usage
            index_usage = database_analysis.get("index_usage", [])
            if index_usage and isinstance(index_usage, list):
                unused = [idx for idx in index_usage if isinstance(idx, dict) and idx.get("scans", 0) == 0]
                if unused:
                    report_lines.append(f"\nUnused Indexes: {len(unused)}")
            
            # Lock contention
            lock_contention = database_analysis.get("lock_contention", [])
            if lock_contention and isinstance(lock_contention, list) and len(lock_contention) > 0:
                report_lines.append(f"\nLock Contention: {len(lock_contention)} blocked queries detected")
            else:
                report_lines.append(f"\nLock Contention: None detected")
            
            # Connection stats
            conn_stats = database_analysis.get("connection_stats", {})
            if conn_stats and isinstance(conn_stats, dict):
                total = conn_stats.get("total_connections", 0)
                active = conn_stats.get("active_connections", 0)
                idle = conn_stats.get("idle_connections", 0)
                report_lines.append(f"\nConnections: {total} total ({active} active, {idle} idle)")
            
            # Growth rate
            growth_rate = database_analysis.get("growth_rate", [])
            if growth_rate and isinstance(growth_rate, list):
                report_lines.append(f"\nGrowth Rate:")
                for table in growth_rate:
                    if isinstance(table, dict):
                        name = table.get("table_name", "unknown")
                        rpd = table.get("rows_per_day", 0)
                        if rpd > 0:
                            report_lines.append(f"  {name}: {rpd:,.0f} rows/day")
            
            # Database insights
            db_insights = database_analysis.get("insights", [])
            if db_insights and isinstance(db_insights, list):
                report_lines.append(f"\nDatabase Insights:")
                for insight in db_insights:
                    report_lines.append(f"  {insight}")
        else:
            report_lines.append("Database analysis not available")
    except Exception as e:
        report_lines.append(f"Error displaying database analysis: {str(e)}")
    
    report_lines.append("")

    # Traffic Pattern Analysis Section
    report_lines.append("📊 TRAFFIC PATTERN ANALYSIS")
    report_lines.append("-" * 50)
    
    try:
        if traffic_analysis and isinstance(traffic_analysis, dict):
            # Hourly patterns
            hourly = traffic_analysis.get("hourly_patterns", {})
            if hourly and isinstance(hourly, dict) and "peak_hour" in hourly:
                report_lines.append(f"Peak Hour: {hourly['peak_hour']:02d}:00 ({hourly['peak_runs']} runs)")
                if hourly.get("quiet_hours"):
                    quiet_count = len(hourly["quiet_hours"])
                    report_lines.append(f"Quiet Hours: {quiet_count} hours with minimal activity")
            
            # Concurrent patterns
            concurrent = traffic_analysis.get("concurrent_patterns", {})
            if concurrent and isinstance(concurrent, dict) and "max_concurrent_dags" in concurrent:
                report_lines.append(f"\nConcurrency:")
                report_lines.append(f"  Max Concurrent DAGs: {concurrent['max_concurrent_dags']}")
                report_lines.append(f"  Avg Concurrent DAGs: {concurrent['avg_concurrent_dags']:.1f}")
                report_lines.append(f"  P95 Concurrent DAGs: {concurrent['p95_concurrent_dags']:.1f}")
            
            # Task concurrency
            task_conc = traffic_analysis.get("task_concurrency", {})
            if task_conc and isinstance(task_conc, dict) and "max_concurrent_tasks" in task_conc:
                report_lines.append(f"\nTask Concurrency:")
                report_lines.append(f"  Max Concurrent Tasks: {task_conc['max_concurrent_tasks']}")
                report_lines.append(f"  Avg Concurrent Tasks: {task_conc['avg_concurrent_tasks']:.1f}")
                report_lines.append(f"  P95 Concurrent Tasks: {task_conc['p95_concurrent_tasks']:.1f}")
                
                # Calculate utilization if possible
                try:
                    parallelism = int(conf.get("core", "parallelism", fallback="32"))
                    avg_util = (task_conc['avg_concurrent_tasks'] / parallelism) * 100
                    peak_util = (task_conc['max_concurrent_tasks'] / parallelism) * 100
                    report_lines.append(f"  Avg Utilization: {avg_util:.1f}%")
                    report_lines.append(f"  Peak Utilization: {peak_util:.1f}%")
                except Exception:
                    pass
            
            # High frequency DAGs
            dag_freq = traffic_analysis.get("dag_frequency", [])
            if dag_freq and isinstance(dag_freq, list):
                high_freq = [d for d in dag_freq if isinstance(d, dict) and d.get("runs_per_day", 0) > 50]
                if high_freq:
                    report_lines.append(f"\nHigh Frequency DAGs ({len(high_freq)}):")
                    for dag in high_freq[:5]:
                        report_lines.append(f"  - {dag['dag_id']}: {dag['runs_per_day']:.1f} runs/day")
            
            # Traffic insights
            insights = traffic_analysis.get("insights", [])
            if insights and isinstance(insights, list):
                report_lines.append(f"\nTraffic Insights:")
                for insight in insights:
                    report_lines.append(f"  {insight}")
        else:
            report_lines.append("Traffic analysis not available")
    except Exception as e:
        report_lines.append(f"Error displaying traffic analysis: {str(e)}")
    
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

    # Error pattern recommendations
    if error_pattern_analysis and isinstance(error_pattern_analysis, dict):
        error_insights = error_pattern_analysis.get("insights", [])
        if isinstance(error_insights, list):
            critical_errors = [i for i in error_insights if "🚨" in i]
            if critical_errors:
                recommendations.append(
                    f"🚨 URGENT: {len(critical_errors)} critical error patterns - prioritize fixes"
                )
        
        # Check for cascades
        cascades = error_pattern_analysis.get("failure_cascades", [])
        if isinstance(cascades, list) and len(cascades) > 0:
            recommendations.append(
                f"Improve error handling - {len(cascades)} DAGs have failure cascades"
            )

    # Scheduler performance recommendations
    if scheduler_analysis and isinstance(scheduler_analysis, dict):
        sched_insights = scheduler_analysis.get("insights", [])
        if isinstance(sched_insights, list):
            critical_sched = [i for i in sched_insights if "🚨" in i]
            if critical_sched:
                recommendations.append(
                    f"🚨 URGENT: {len(critical_sched)} critical scheduler issues - may cause DAG processing failures"
                )

    # Prioritize configuration issues
    config_issues = config_analysis.get("issues", [])
    critical_config_issues = [issue for issue in config_issues if "🚨 CRITICAL" in issue]
    warning_config_issues = [issue for issue in config_issues if "⚠️ WARNING" in issue and "🚨 CRITICAL" not in issue]
    
    if critical_config_issues:
        recommendations.append(
            f"🚨 URGENT: Fix {len(critical_config_issues)} critical configuration conflicts that may cause DAG processing failures"
        )
    
    if warning_config_issues:
        recommendations.append(
            f"Review {len(warning_config_issues)} configuration warnings to optimize performance"
        )

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

    # Predictive analytics recommendations
    if predictive_analysis and isinstance(predictive_analysis, dict):
        pred_insights = predictive_analysis.get("insights", [])
        if isinstance(pred_insights, list):
            critical_pred = [i for i in pred_insights if "🚨" in i]
            warning_pred = [i for i in pred_insights if "⚠️" in i and "🚨" not in i]
            
            if critical_pred:
                recommendations.append(
                    f"🚨 URGENT: {len(critical_pred)} critical predictive alerts - take preventive action"
                )
            
            if warning_pred:
                recommendations.append(
                    f"Review {len(warning_pred)} predictive warnings for proactive optimization"
                )
        
        # Check for increasing failures
        failure_preds = predictive_analysis.get("failure_predictions", [])
        if isinstance(failure_preds, list):
            increasing = [t for t in failure_preds if isinstance(t, dict) and t.get("trend") == "Increasing"]
            if len(increasing) > 5:
                recommendations.append(
                    f"Investigate {len(increasing)} tasks with increasing failure rates before they escalate"
                )

    # SLA & timing recommendations
    if sla_timing_analysis and isinstance(sla_timing_analysis, dict):
        sla_insights = sla_timing_analysis.get("insights", [])
        if isinstance(sla_insights, list):
            critical_sla = [i for i in sla_insights if "🚨" in i]
            warning_sla = [i for i in sla_insights if "⚠️" in i and "🚨" not in i]
            
            if critical_sla:
                recommendations.append(
                    f"🚨 URGENT: Address {len(critical_sla)} critical SLA/timing issues"
                )
            
            if warning_sla:
                recommendations.append(
                    f"Review {len(warning_sla)} SLA/timing warnings"
                )
        
        # Check for catchup backlog
        catchup = sla_timing_analysis.get("catchup_backlog", [])
        if isinstance(catchup, list) and len(catchup) > 0:
            total_queued = sum(c.get("queued_runs", 0) for c in catchup if isinstance(c, dict))
            if total_queued > 50:
                recommendations.append(
                    f"Clear catchup backlog - {total_queued} queued runs detected"
                )

    # Task execution recommendations
    if task_execution_analysis and isinstance(task_execution_analysis, dict):
        task_insights = task_execution_analysis.get("insights", [])
        if isinstance(task_insights, list):
            critical_task = [i for i in task_insights if "🚨" in i]
            warning_task = [i for i in task_insights if "⚠️" in i and "🚨" not in i]
            
            if critical_task:
                recommendations.append(
                    f"🚨 URGENT: Address {len(critical_task)} critical task execution issues"
                )
            
            if warning_task:
                recommendations.append(
                    f"Review {len(warning_task)} task execution warnings"
                )
        
        # Check for zombie tasks
        zombie_tasks = task_execution_analysis.get("zombie_tasks", [])
        if isinstance(zombie_tasks, list) and len(zombie_tasks) > 0:
            recommendations.append(
                f"Clear {len(zombie_tasks)} zombie tasks stuck in running state"
            )
        
        # Check queue depth
        queue_depth = task_execution_analysis.get("queue_depth", {})
        if isinstance(queue_depth, dict):
            max_queued = queue_depth.get("max_queued", 0)
            if max_queued > 100:
                recommendations.append(
                    "Increase parallelism or worker capacity - severe queue bottleneck detected"
                )

    # Database performance recommendations
    if database_analysis and isinstance(database_analysis, dict):
        db_insights = database_analysis.get("insights", [])
        if isinstance(db_insights, list):
            critical_db = [i for i in db_insights if "🚨" in i]
            warning_db = [i for i in db_insights if "⚠️" in i and "🚨" not in i]
            
            if critical_db:
                recommendations.append(
                    f"🚨 URGENT: Address {len(critical_db)} critical database issues"
                )
            
            if warning_db:
                recommendations.append(
                    f"Review {len(warning_db)} database warnings for optimization"
                )
        
        # Check specific database metrics
        row_counts = database_analysis.get("row_counts", [])
        if isinstance(row_counts, list):
            for table in row_counts:
                if isinstance(table, dict):
                    if table.get("table_name") == "task_instance" and table.get("row_count", 0) > 5_000_000:
                        recommendations.append(
                            "Implement task_instance cleanup - table exceeds 5M rows"
                        )
                        break

    # Traffic pattern recommendations
    if traffic_analysis and isinstance(traffic_analysis, dict):
        traffic_insights = traffic_analysis.get("insights", [])
        if isinstance(traffic_insights, list):
            critical_traffic = [i for i in traffic_insights if "🚨" in i]
            warning_traffic = [i for i in traffic_insights if "⚠️" in i and "🚨" not in i]
            
            if critical_traffic:
                recommendations.append(
                    f"Address {len(critical_traffic)} critical traffic/concurrency issues"
                )
            
            if warning_traffic:
                recommendations.append(
                    f"Review {len(warning_traffic)} traffic pattern warnings for optimization opportunities"
                )
        
        # Check for specific patterns
        concurrent = traffic_analysis.get("concurrent_patterns", {})
        if isinstance(concurrent, dict) and concurrent.get("max_concurrent_dags", 0) > 50:
            recommendations.append(
                "High concurrent DAG execution detected - verify scheduler and worker capacity"
            )
        
        task_conc = traffic_analysis.get("task_concurrency", {})
        if isinstance(task_conc, dict) and task_conc.get("max_concurrent_tasks", 0) > 100:
            recommendations.append(
                "High task concurrency detected - ensure adequate parallelism configuration"
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
            "traffic_insights": len(traffic_analysis.get("insights", [])) if traffic_analysis else 0,
            "peak_concurrent_dags": traffic_analysis.get("concurrent_patterns", {}).get("max_concurrent_dags", 0) if traffic_analysis else 0,
            "peak_concurrent_tasks": traffic_analysis.get("task_concurrency", {}).get("max_concurrent_tasks", 0) if traffic_analysis else 0,
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
    schedule=None,  # Manual trigger (compatible with Airflow 2.4+ and 3.x)
    catchup=False,
    tags=["diagnostics", "mwaa", "monitoring", "configuration"],
    params={"analysis_days": 7, "env_name": ENV_NAME, "s3_bucket": S3_BUCKET},
    doc_md="""
    ## DAG Diagnostics
    
    This DAG provides comprehensive diagnostics for MWAA environments including:
    - Airflow configuration change detection and conflict analysis
    - DAG serialization analysis
    - Performance monitoring
    - Failure pattern analysis
    - Serialization instability detection
    - Traffic pattern and concurrency analysis
    - System metrics collection
    
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
    error_pattern_analysis = analyze_error_patterns()
    scheduler_analysis = analyze_scheduler_performance()
    predictive_analysis = analyze_predictive_patterns()
    sla_timing_analysis = analyze_sla_and_timing()
    task_execution_analysis = analyze_task_execution_patterns()
    database_analysis = analyze_database_performance()
    traffic_analysis = analyze_traffic_patterns()
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
        error_pattern_analysis,
        scheduler_analysis,
        predictive_analysis,
        sla_timing_analysis,
        task_execution_analysis,
        database_analysis,
        traffic_analysis,
        system_metrics,
    ]
    (
        [
            config_capture,
            dag_analysis,
            performance_analysis,
            failure_analysis,
            instability_analysis,
            error_pattern_analysis,
            scheduler_analysis,
            predictive_analysis,
            sla_timing_analysis,
            task_execution_analysis,
            database_analysis,
            traffic_analysis,
            system_metrics,
        ]
        >> report
        >> cleanup
    )


# Instantiate the DAG
dag_diagnostics_dag_instance = dag_diagnostics_dag()
