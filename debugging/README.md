# DAG Diagnostics System

A comprehensive diagnostics DAG for Apache Airflow/MWAA environments that analyzes performance, configuration, and system health.

## What It Does

This DAG provides deep insights into your Airflow environment by analyzing:

- **Configuration Issues**: Detects conflicts and misconfigurations that can cause failures
- **DAG Performance**: Identifies slow, failing, or problematic DAGs
- **Database Health**: Monitors table sizes, connections, and growth patterns
- **System Metrics**: Collects CPU, memory, disk, and process information
- **Traffic Patterns**: Analyzes workload distribution and concurrency
- **Predictive Analytics**: Forecasts capacity needs and identifies degradation trends

## Quick Start

### 1. Upload the DAG

Copy `dag_diagnostics.py` to your DAGs folder.

### 2. Set Required Variables (Optional)

```bash
# Environment name for reports
airflow variables set dag_diagnostics_env_name "MyEnvironment"

# S3 bucket for saving reports
airflow variables set dag_diagnostics_s3_bucket "my-diagnostics-bucket"

# Analysis period in days (default: 7)
airflow variables set dag_diagnostics_analysis_days 14
```

### 3. Run the DAG

**Option A: Use defaults**
```bash
airflow dags trigger dag_diagnostics
```

**Option B: Custom parameters**
```bash
airflow dags trigger dag_diagnostics --conf '{
  "analysis_days": 14,
  "env_name": "Production",
  "s3_bucket": "my-reports-bucket"
}'
```

### 4. View Results

- Check the DAG logs for the complete diagnostics report
- If S3 is configured, the report is also saved to S3
- Look for 🚨 **URGENT** and ⚠️ **WARNING** items in the output

## Sample Output

```
🚨 URGENT: Fix 2 critical configuration conflicts that may cause DAG processing failures
⚠️ High traffic during peak hour (09:00) with 156 runs - consider load balancing
💡 3 tasks showing increasing failure rates - investigate before failures escalate
✅ No significant database issues detected
```

## Configuration

The DAG is designed to work out-of-the-box with minimal setup. All configuration is optional:

| Variable | Default | Description |
|----------|---------|-------------|
| `dag_diagnostics_env_name` | `""` | Environment name for reports |
| `dag_diagnostics_s3_bucket` | `""` | S3 bucket for saving reports |
| `dag_diagnostics_analysis_days` | `7` | Number of days to analyze |

## Requirements

- Apache Airflow 2.7+ or 3.10+
- PostgreSQL database (MWAA compatible)
- Optional: S3 access for report storage

## Troubleshooting

**DAG fails to start:**
- Ensure the DAG file is in your DAGs folder
- Check that your Airflow version is supported (2.7+ or 3.10+)

**Database connection errors:**
- The DAG automatically creates a temporary connection using MWAA environment variables
- No manual database setup required

**S3 upload fails:**
- Check that `aws_default` connection is configured
- Verify S3 bucket exists and is accessible
- Reports will still appear in DAG logs even if S3 fails

## What's Analyzed

- **Configuration**: 40+ critical Airflow settings and conflict detection
- **Performance**: DAG success rates, runtime trends, failure patterns
- **Database**: Table sizes, connection usage, growth rates, lock contention
- **System**: CPU, memory, disk usage, process information
- **Traffic**: Hourly patterns, concurrency, queue depth
- **Predictive**: Failure forecasting, capacity planning, anomaly detection

The DAG generates actionable recommendations prioritized by urgency level.
