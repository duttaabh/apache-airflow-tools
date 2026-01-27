# AWS MWAA Custom RBAC Solution - Airflow 3.x

This folder contains the Airflow 3.x compatible version of the MWAA Custom RBAC solution.

## Key Differences from Airflow 2.x

### 1. Database Connection Handling

**Airflow 3.x Changes:**
- Environment variable changed from `AIRFLOW__CORE__SQL_ALCHEMY_CONN` to `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
- MWAA may block direct SQL Alchemy connection access with `airflow-db-not-allowed` prefix
- New MWAA-specific environment variables available:
  - `DB_SECRETS`: JSON string containing database credentials
  - `POSTGRES_HOST`: Database host
  - `POSTGRES_PORT`: Database port (default: 5432)
  - `POSTGRES_DB`: Database name (default: AirflowMetadata)

**Implementation:**
The `create_role_glue_job_dag.py` now handles both Airflow 2.x and 3.x connection methods:
```python
# Try Airflow 2.x style first
sql_alchemy_conn = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
if sql_alchemy_conn is None:
    # Try Airflow 3.x style
    sql_alchemy_conn = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")

# If blocked or unavailable, use MWAA-specific variables
if sql_alchemy_conn is None or sql_alchemy_conn.startswith("airflow-db-not-allowed"):
    mwaa_db_credentials = os.getenv("DB_SECRETS")
    mwaa_db_host = os.getenv("POSTGRES_HOST")
    # Parse and construct JDBC URL
```

### 2. DAG Definition

**Airflow 3.x Changes:**
- Prefer `@dag` decorator over context manager style
- Use `schedule=None` instead of `schedule_interval=None`
- DAG instantiation required at module level

**Implementation:**
```python
# Airflow 2.x style (still works)
with DAG(dag_id='my_dag', schedule_interval=None) as dag:
    task1 = ...

# Airflow 3.x style (preferred)
@dag(dag_id='my_dag', schedule=None)
def my_dag():
    task1 = ...
    return task1

my_dag_instance = my_dag()
```

### 3. Metadata Database Schema

**Airflow 3.x Changes:**
- New tables: `dag_version`, `asset`, `asset_event`, `backfill`, `backfill_dag_run`
- Updated tables: `dag_run`, `task_instance`, `task_instance_history`
- New date fields for filtering

The RBAC tables (`ab_role`, `ab_permission`, etc.) remain the same, so role creation logic is unchanged.

### 4. Operator Changes

**Airflow 3.x Changes:**
- `DummyOperator` deprecated, use `EmptyOperator` instead
- Some provider packages have new versions
- Task decorators (`@task`) are now the preferred pattern

## Files

### CloudFormation Templates
- `01-vpc-mwaa.yml` - VPC and MWAA environment (same as Airflow 2.x)
- `02-alb.yml` - ALB and authentication (same as Airflow 2.x)

### DAG Files
- `role_creation_dag/create_role_glue_job_dag.py` - **Updated for Airflow 3.x** with new connection handling
- `role_creation_dag/update_user_role_dag.py` - User role management (same as Airflow 2.x)
- `sample_dags/hello_world_simple.py` - Simple example DAG (same as Airflow 2.x)
- `sample_dags/hello_world_advanced.py` - Advanced example DAG (same as Airflow 2.x)

### Lambda Function
- `lambda_auth/lambda_mwaa-authorizer.py` - Authentication Lambda (same as Airflow 2.x)

### Scripts
- `deploy-stack.sh` - Deployment script (same as Airflow 2.x)
- `cleanup-stack.sh` - Cleanup script (same as Airflow 2.x)

### Configuration
- `deployment-config.json` - Deployment parameters (same as Airflow 2.x)
- `deployment-config.template.json` - Template for configuration (same as Airflow 2.x)

## Deployment

The deployment process is identical to Airflow 2.x:

```bash
# Full deployment
./deploy-stack.sh --vpc-stack mwaa-af3 --alb-stack mwaa-af3-alb

# Or step by step
./deploy-stack.sh --vpc-stack mwaa-af3 --vpc
./deploy-stack.sh --vpc-stack mwaa-af3 --upload
./deploy-stack.sh --vpc-stack mwaa-af3 --alb-stack mwaa-af3-alb --alb
```

## Testing

After deployment, test the role creation DAG:

```json
{
  "aws_region": "us-east-1",
  "stack_name": "mwaa-af3",
  "source_role": "User",
  "target_role": "MWAARestrictedTest",
  "specific_dags": ["hello_world_advanced", "hello_world_simple"]
}
```

The DAG will automatically detect whether it's running on Airflow 2.x or 3.x and use the appropriate connection method.

## Backward Compatibility

The updated `create_role_glue_job_dag.py` is backward compatible with Airflow 2.x. It will:
1. Try Airflow 2.x environment variables first
2. Fall back to Airflow 3.x environment variables
3. Use MWAA-specific variables if SQL Alchemy connection is blocked

This means you can use the same DAG file for both Airflow 2.x and 3.x environments.

## Migration from Airflow 2.x

To migrate from Airflow 2.x to 3.x:

1. **Update MWAA Environment:**
   - Change Airflow version in CloudFormation template or AWS Console
   - MWAA will handle the upgrade automatically

2. **Update DAGs (Optional):**
   - Replace `DummyOperator` with `EmptyOperator`
   - Update `schedule_interval` to `schedule`
   - Consider using `@dag` decorator for new DAGs

3. **Test Role Creation:**
   - The role creation DAG should work without changes
   - Test with a non-production role first

4. **Update Custom DAGs:**
   - Review DAGs for deprecated operators
   - Test thoroughly in a development environment

## Troubleshooting

### Connection Issues

If you see errors related to database connections:

```
AirflowException: Neither SQL_ALCHEMY_CONN nor MWAA database environment variables are available.
```

**Solution:** Check that your MWAA environment is properly configured. In Airflow 3.x, MWAA may block direct SQL Alchemy access. The DAG will automatically use MWAA-specific environment variables.

### Role Creation Fails

If the Glue job fails to create roles:

1. Check Glue job logs in CloudWatch
2. Verify the Glue connection is created successfully
3. Ensure the GlueRoleCreatorRole has necessary permissions
4. Check that the source role exists in MWAA

## References

- [AWS MWAA Documentation](https://docs.aws.amazon.com/mwaa/)
- [Integrate Amazon MWAA with Microsoft Entra ID using SAML authentication](https://aws.amazon.com/blogs/big-data/integrate-amazon-mwaa-with-microsoft-entra-id-using-saml-authentication/) - Azure AD integration guide
- [Limit access to Apache Airflow DAGs in Amazon MWAA](https://docs.aws.amazon.com/mwaa/latest/userguide/limit-access-to-dags.html) - Custom role creation tutorial
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [MWAA Airflow 3.x Migration Guide](https://docs.aws.amazon.com/mwaa/latest/userguide/airflow-versions.html)
- [Airflow 3.0 Release Notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html)
