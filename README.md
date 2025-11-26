# Apache Airflow Utilities

> **Found these tools helpful?** Give us a ⭐ on GitHub to show your support and help others discover these utilities!

A collection of **non-production-ready** tools and plugins for Apache Airflow and AWS MWAA (Managed Workflows for Apache Airflow).

## Tools Overview

### Database Export
Export MWAA metadata database tables to S3 using AWS Glue for backup, analysis, or migration purposes.

### Diagnostics & Monitoring
Comprehensive DAG health monitoring system with performance metrics, failure analysis, and database health tracking.

### User Management Plugins
Tools for handling usernames with special characters and automating user creation workflows in Airflow's REST API.

## Quick Start

```bash
# Clone the repository
git clone <repository-url>
cd <repository-name>

# Choose the tool you need:
# - Database Export: See db_export/
# - Diagnostics: See debugging/
# - User Plugins: See plugins/
```

## Components

### 1. Database Export (`db_export/`)

Export MWAA metadata tables to S3 for backup or analysis.

**Features:**
- Automatic Glue connection creation to MWAA metadata database
- Time-windowed exports (configurable retention)
- Exports: dag_run, task_fail, task_instance, serialized_dag
- CSV output to S3

**Quick Setup:**
```bash
# 1. Set Airflow Variables
airflow variables set aws_region "us-east-1"
airflow variables set glue_role_name "your-glue-role"
airflow variables set export_s3_bucket "your-bucket"

# 2. Copy DAG to Airflow
cp db_export/glue_mwaa_export_v2.py $AIRFLOW_HOME/dags/

# 3. Trigger the DAG
airflow dags trigger glue_mwaa_export_dag
```

[Full Documentation →](db_export/)

### 2. Diagnostics System (`debugging/`)

Comprehensive monitoring and analysis for Airflow environments.

**Capabilities:**
- **DAG Analysis**: Size, complexity, serialization patterns
- **Performance Metrics**: Success rates, runtime statistics, trends
- **Failure Analysis**: Task failures, retry patterns, SLA misses
- **Traffic Patterns**: Hourly load, concurrent runs, queue depth
- **Database Health**: Table sizes, index usage, lock contention, growth rates
- **Resource Utilization**: Pool usage, executor lag, zombie tasks

**Quick Setup:**
```bash
# 1. Set Airflow Variables (optional, has defaults)
airflow variables set dag_diagnostics_analysis_days "7"
airflow variables set dag_diagnostics_s3_bucket "your-bucket"

# 2. Copy DAG to Airflow
cp debugging/system_diagnostics/dag_diagnostics.py $AIRFLOW_HOME/dags/

# 3. Run diagnostics
airflow dags trigger dag_diagnostics
```

[Full Documentation →](debugging/)

### 3. User Management Plugins (`plugins/`)

#### User Creation Interceptor

Demonstrates how to intercept user creation events to execute custom logic using SQLAlchemy event listeners.

**Pattern demonstrated:** Username sanitization (replacing `/` with `_`)

**MWAA Warning:** Username sanitization breaks IAM mapping in MWAA. Use this plugin as a template for other user creation workflows.

**Adaptable for:**
- Automatic role assignment based on user attributes
- Triggering provisioning workflows (Slack notifications, S3 setup, etc.)
- Username validation and policy enforcement
- Audit logging and compliance tracking
- Integration with external identity systems (LDAP, Active Directory, SSO)

**Setup:**
```bash
cp plugins/user_creation_interceptor/user_creation_interceptor.py $AIRFLOW_HOME/plugins/
airflow webserver -D && airflow scheduler -D
```

**Example (Username Sanitization - Self-hosted Airflow only):**
```bash
# Create user with slash
airflow users create --username "domain/username" ...
# Actually created as: domain_username
```

[Full Documentation →](plugins/user_creation_interceptor/)

#### Username API Extension

Provides alternative API endpoints using query parameters to preserve exact usernames with special characters.

**Use when:**
- You have existing users with slashes in usernames
- You must preserve exact usernames (especially for MWAA/IAM mapping)
- You can update API clients to use new endpoints

**Endpoints:**
- `GET /api/v1/users-ext/?username=domain/username`
- `PATCH /api/v1/users-ext/?username=domain/username`
- `DELETE /api/v1/users-ext/?username=domain/username`

**Setup:**
```bash
cp plugins/username_api_extension/username_api_extension.py $AIRFLOW_HOME/plugins/
airflow webserver -D && airflow scheduler -D
```

**Example:**
```bash
# Access user with slash via extended API
curl "http://localhost:8080/api/v1/users-ext/?username=domain%2Fusername" \
  -u "admin:admin"
```

[Full Documentation →](plugins/username_api_extension/)

## Compatibility

| Component | Airflow Version | Python | Database | Auth Manager | MWAA Compatible |
|-----------|----------------|--------|----------|--------------|-----------------|
| DB Export | 2.7+ | 3.8+ | PostgreSQL | Any | Yes |
| Diagnostics | 2.7+, 3.10+ | 3.8+ | PostgreSQL, MySQL, SQLite | Any | Yes |
| User Creation Interceptor | 2.8+ | 3.8+ | PostgreSQL, MySQL, SQLite | FAB | Pattern: Yes, Username sanitization: No |
| Username API Extension | 2.8+ | 3.8+ | PostgreSQL, MySQL, SQLite | FAB | Yes |

## Requirements

### Common
- Apache Airflow 2.7+
- Python 3.8+

### DB Export Specific
- AWS MWAA environment
- AWS Glue access
- S3 bucket for exports
- IAM role with Glue and S3 permissions

### Diagnostics Specific
- PostgreSQL connection (for full features)
- S3 bucket (optional, for report storage)

### User Plugins Specific
- FAB (Flask-AppBuilder) auth manager
- Airflow 2.8+ for plugin support

## Installation

Each component is self-contained. Install only what you need:

```bash
# Database Export
cp db_export/glue_mwaa_export_v2.py $AIRFLOW_HOME/dags/

# Diagnostics
cp debugging/system_diagnostics/dag_diagnostics.py $AIRFLOW_HOME/dags/

# User Plugins (choose one or both)
cp plugins/user_creation_interceptor/user_creation_interceptor.py $AIRFLOW_HOME/plugins/
cp plugins/username_api_extension/username_api_extension.py $AIRFLOW_HOME/plugins/
```

## Testing

Each component includes tests:

```bash
# User Creation Interceptor
python plugins/user_creation_interceptor/test_user_interceptor.py

# Username API Extension
bash plugins/username_api_extension/test_api_extension.sh
```

## Configuration

All tools use Airflow Variables for configuration, making them easy to customize without code changes.

### DB Export Variables
```python
aws_region = "us-east-1"
glue_role_name = "your-iam-role"
export_s3_bucket = "your-bucket"
database_name = "airflow_metadb"
```

### Diagnostics Variables
```python
dag_diagnostics_analysis_days = 7
dag_diagnostics_s3_bucket = "your-bucket"
dag_diagnostics_large_dag_threshold_mb = 5.0
dag_diagnostics_critical_dag_threshold_mb = 10.0
dag_diagnostics_min_success_rate = 95.0
```

## Use Cases

### Backup & Recovery
Use DB Export to regularly backup metadata for disaster recovery or environment migration.

### Performance Optimization
Use Diagnostics to identify slow DAGs, resource bottlenecks, and optimization opportunities.

### Troubleshooting
Use Diagnostics to investigate failures, analyze retry patterns, and detect zombie tasks.

### User Management & Automation
Use User Creation Interceptor to automate role assignment, provisioning, and validation workflows. Use Username API Extension to handle enterprise authentication systems that use domain/username formats while preserving exact usernames.

## Architecture

```
airflow-utilities/
├── db_export/              # Metadata export to S3
│   └── glue_mwaa_export_v2.py
├── debugging/              # Diagnostics & monitoring
│   ├── README.md
│   └── system_diagnostics/
│       └── dag_diagnostics.py
└── plugins/                # User management plugins
    ├── user_creation_interceptor/
    │   ├── user_creation_interceptor.py
    │   ├── README.md
    │   └── test_user_interceptor.py
    └── username_api_extension/
        ├── username_api_extension.py
        ├── README.md
        └── test_api_extension.sh
```

## Contributing

Contributions are welcome! Each component is independent, so you can contribute to specific areas:

- Add new diagnostic queries to the DiagnosticQueries registry
- Extend export capabilities with new tables or formats
- Add customization examples for user plugins
- Improve documentation and examples
- Report issues or suggest enhancements

## Disclaimer

**This code is provided "as is" without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and noninfringement. In no event shall the authors or copyright holders be liable for any claim, damages, or other liability, whether in an action of contract, tort, or otherwise, arising from, out of, or in connection with the software or the use or other dealings in the software.**

**Use at your own risk.** These tools are provided as examples and demonstrations. Always test thoroughly in a non-production environment before deploying to production. The authors and contributors are not responsible for any data loss, system downtime, or other issues that may arise from using these tools.

## License

Apache License 2.0 - Same as Apache Airflow

## Support

For issues or questions:

1. Check component-specific README files
2. Review test files for usage examples
3. Check Airflow logs for detailed error messages

## Security

- All tools use Airflow's built-in authentication and authorization
- Database credentials are retrieved from Airflow connections
- No sensitive data is logged
- All operations are audited in Airflow logs

## Performance

- **DB Export**: Minimal impact, runs as scheduled job
- **Diagnostics**: Read-only queries, configurable analysis window
- **User Plugins**: <1ms overhead per user operation

## Best Practices

1. **Test Thoroughly**: Always test in non-production environments first
2. **Start Small**: Begin with short analysis windows before expanding
3. **Monitor Resources**: Watch database load during diagnostics runs
4. **Regular Exports**: Schedule DB exports for consistent backups
5. **Review Logs**: Check plugin logs after installation to verify proper loading
6. **Understand Limitations**: Read component-specific documentation, especially MWAA compatibility notes
7. **Backup First**: Create backups before making changes to user management or database operations

## Roadmap

Potential future enhancements:

- Additional export formats (Parquet, JSON)
- Real-time alerting for diagnostics
- Grafana/Prometheus integration
- Additional user plugin customization options
- Support for more authentication backends

---

**Quick Links:**
- [DB Export Documentation](db_export/)
- [Diagnostics Documentation](debugging/)
- [User Creation Interceptor](plugins/user_creation_interceptor/)
- [Username API Extension](plugins/username_api_extension/)
