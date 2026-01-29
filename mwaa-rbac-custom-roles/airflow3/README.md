# AWS MWAA Custom RBAC Solution - Airflow 3.x

This folder contains the Airflow 3.x compatible version of the MWAA Custom RBAC solution.

## Key Differences from Airflow 2.x

### 1. Authentication Endpoints

**Airflow 3.x Changes:**
- New login endpoint: `/auth/login/` (replaces `/login/`)
- New plugin path: `/pluginsv2/aws_mwaa/aws-console-sso` (in addition to `/aws_mwaa/aws-console-sso`)
- MWAA `CreateWebLoginToken` API remains unchanged

**Implementation:**
The ALB listener rules have been updated to handle Airflow 3.x authentication paths:
- **ListenerRule5 (Priority 5)**: Intercepts `/auth/login/` and forwards to Lambda
- Default action: Forwards all unmatched paths (including `/pluginsv2/*`) to MWAA target group

### 2. DAG Definition

**Airflow 3.x Changes:**
- **BREAKING**: `schedule_interval` parameter removed, use `schedule` instead
- **BREAKING**: `provide_context=True` parameter removed from PythonOperator (context always provided)
- **BREAKING**: `DummyOperator` removed, use `EmptyOperator` instead
- Prefer `@dag` decorator over context manager style (optional)

**Implementation:**
All DAG files have been updated for Airflow 3.x compatibility:
```python
# Airflow 2.x (deprecated)
with DAG(dag_id='my_dag', schedule_interval=None) as dag:
    task1 = PythonOperator(
        task_id='task1',
        python_callable=my_func,
        provide_context=True,  # Removed in Airflow 3.x
    )

# Airflow 3.x (required)
with DAG(dag_id='my_dag', schedule=None) as dag:
    task1 = PythonOperator(
        task_id='task1',
        python_callable=my_func,  # Context always provided
    )
```

### 3. Database Connection Handling

**Airflow 3.x Changes:**
- Environment variable changed from `AIRFLOW__CORE__SQL_ALCHEMY_CONN` to `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
- MWAA blocks direct SQL Alchemy connection access with `airflow-db-not-allowed:///` connection string
- MWAA provides database credentials via environment variables:
  - `DB_SECRETS`: JSON string containing `{"username": "...", "password": "..."}`
  - `POSTGRES_HOST`: Database host
  - `POSTGRES_PORT`: Database port (default: 5432)
  - `POSTGRES_DB`: Database name (default: AirflowMetadata)

**Implementation:**
The `update_user_role_dag.py` now uses direct PostgreSQL database access via `psycopg2`:
```python
# Get database credentials from MWAA environment variables
db_secrets = json.loads(os.getenv("DB_SECRETS"))
db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT", "5432")
db_name = os.getenv("POSTGRES_DB", "AirflowMetadata")

# Connect directly to PostgreSQL
conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    database=db_name,
    user=db_secrets["username"],
    password=db_secrets["password"]
)
```

The `create_role_glue_job_dag.py` handles both Airflow 2.x and 3.x connection methods:
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

**Important:** The `psycopg2` library is pre-installed in MWAA Airflow 3.0.6 via `apache-airflow-providers-postgres==6.2.1`, so no additional dependencies are required.

### 4. Metadata Database Schema

**Airflow 3.x Changes:**
- New tables: `dag_version`, `asset`, `asset_event`, `backfill`, `backfill_dag_run`
- Updated tables: `dag_run`, `task_instance`, `task_instance_history`
- New date fields for filtering

The RBAC tables (`ab_role`, `ab_permission`, `ab_view_menu`, `ab_permission_view`, `ab_permission_view_role`) remain the same, so role creation logic is unchanged.

### 5. REST API Changes

**Airflow 3.x Changes:**
- `/users` endpoint removed (404 error)
- DAG trigger requires `logical_date` field in request body (422 error without it)
- Response format remains the same

**Implementation:**
The Lambda authorizer has been updated:
```python
# Airflow 3.x requires logical_date when triggering DAGs
from datetime import datetime, timezone
request_body = {
    'conf': {
        'username': username,
        'role': role
    },
    'logical_date': datetime.now(timezone.utc).isoformat()
}
```

The `check_user_role()` function has been removed since the `/users` endpoint is not available in Airflow 3.x. The DAG now always creates/updates users without pre-checking.

## Key Features and Fixes

### Airflow 3.x Specific Improvements

1. **Authentication Flow:**
   - Lambda handles new `/auth/login/` endpoint
   - Supports both legacy and new plugin paths
   - 60-second timeout for DAG completion during login

2. **Database Access:**
   - Direct PostgreSQL access via `psycopg2` (pre-installed)
   - Bypasses `airflow-db-not-allowed` restriction
   - Uses MWAA environment variables for credentials

3. **Role Management:**
   - DAG removes ALL existing roles before assigning custom role
   - Ensures users have exactly ONE role
   - Supports usernames with special characters (e.g., `assumed-role/RoleName`)

4. **REST API Compatibility:**
   - Includes `logical_date` field when triggering DAGs (required in Airflow 3.x)
   - Removed `/users` endpoint dependency (not available in Airflow 3.x)
   - Lambda uses user's IAM role for both `create_web_login_token` and `invoke_rest_api`

5. **Custom Role Creation:**
   - Glue job preserves `menu access on DAGs` permission (required to view DAGs page)
   - Excludes specific DAG permissions (`DAG:*`) but keeps general DAG menu access
   - Adds back only specified DAG permissions

### Known Issues and Solutions

**Issue:** Custom role users can't see DAGs in UI

**Root Cause:** The `create_role_glue_job` DAG was excluding `menu access on DAGs` permission

**Solution:** Updated Glue script to keep general DAG permissions while excluding specific DAG permissions:
```python
# Old (incorrect) - excludes all DAG-related permissions
WHERE vm.name NOT LIKE 'DAG:%' AND vm.name NOT LIKE 'DAGs%'

# New (correct) - only excludes specific DAG permissions
WHERE vm.name NOT LIKE 'DAG:%'
```

**Issue:** Users have both Public and custom roles

**Root Cause:** DAG wasn't removing all existing roles before assigning the custom role

**Solution:** Updated `update_user_role_dag.py` to remove ALL roles:
```python
# Remove ALL existing roles
cursor.execute("DELETE FROM ab_user_role WHERE user_id = %s", (user_id,))

# Add ONLY the new custom role
cursor.execute(
    "INSERT INTO ab_user_role (user_id, role_id) VALUES (%s, %s)",
    (user_id, role_id)
)
```

### CloudFormation Templates
- `01-vpc-mwaa.yml` - VPC and MWAA environment (same as Airflow 2.x)
- `02-alb.yml` - ALB and authentication (same as Airflow 2.x)

### DAG Files
- `role_creation_dag/create_role_glue_job_dag.py` - **Updated for Airflow 3.x** with new connection handling
- `role_creation_dag/update_user_role_dag.py` - **Updated for Airflow 3.x** (removed `schedule_interval`, `provide_context`)
- `sample_dags/hello_world_simple.py` - **Updated for Airflow 3.x** (uses `schedule`, no `provide_context`)
- `sample_dags/hello_world_advanced.py` - **Updated for Airflow 3.x** (uses `EmptyOperator`, `schedule`, no `provide_context`)

### Lambda Function
- `lambda_auth/lambda_mwaa-authorizer.py` - **Updated for Airflow 3.x** authentication endpoints

### Scripts
- `deploy-stack.sh` - **Updated** deployment script with DAG file upload support
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

# Update Lambda code only
./deploy-stack.sh --vpc-stack mwaa-af3 --alb-stack mwaa-af3-alb --update-lambda-code
```

### Post-Deployment Steps

After deploying the infrastructure:

1. **Create Custom Role:**
   - Trigger the `create_role_glue_job` DAG in Airflow UI
   - Use configuration:
     ```json
     {
       "aws_region": "us-east-1",
       "stack_name": "mwaa-af3",
       "source_role": "User",
       "target_role": "MWAARestrictedTest",
       "specific_dags": ["hello_world_advanced", "hello_world_simple"]
     }
     ```
   - Wait for Glue job to complete (check CloudWatch logs)

2. **Verify Role Creation:**
   - Go to Security → List Roles in Airflow UI
   - Verify `MWAARestrictedTest` role exists
   - Check that it has `menu access on DAGs` permission

3. **Test Login:**
   - Login with a user in the custom Azure AD group
   - Verify the `update_user_role` DAG runs automatically
   - Check that user has ONLY the `MWAARestrictedTest` role (no Public role)
   - Verify user can see the DAGs page and the specified DAGs

### Troubleshooting Deployment

**Issue:** Lambda function has placeholder code

**Solution:** Run the update Lambda code command:
```bash
./deploy-stack.sh --vpc-stack mwaa-af3 --alb-stack mwaa-af3-alb --update-lambda-code
```

**Issue:** DAG files not uploaded to S3

**Solution:** Run the upload command:
```bash
./deploy-stack.sh --vpc-stack mwaa-af3 --upload
```

## Testing

After deployment, test the complete authentication and role assignment flow:

### 1. Test Role Creation

Trigger the `create_role_glue_job` DAG with configuration:

```json
{
  "aws_region": "us-east-1",
  "stack_name": "mwaa-af3",
  "source_role": "User",
  "target_role": "MWAARestrictedTest",
  "specific_dags": ["hello_world_advanced", "hello_world_simple"]
}
```

**Expected Results:**
- Glue job completes successfully
- Role `MWAARestrictedTest` created in Airflow
- Role has permissions from User role
- Role has `menu access on DAGs` permission
- Role has specific permissions for `hello_world_advanced` and `hello_world_simple` DAGs only

### 2. Test User Login and Role Assignment

Login with a user in the custom Azure AD group:

**Expected Flow:**
1. User authenticates via Azure AD
2. ALB forwards to Lambda authorizer
3. Lambda gets `mwaa_role = "MWAARestrictedTest"` from GROUP_TO_ROLE_MAP
4. Lambda checks if role is in standard roles (Admin, Op, User, Viewer) - it's NOT
5. Lambda triggers `update_user_role` DAG with `role="MWAARestrictedTest"`
6. DAG removes ALL existing roles from user
7. DAG adds ONLY the `MWAARestrictedTest` role
8. Lambda waits up to 60 seconds for DAG completion
9. User is redirected to Airflow UI

**Verification:**
- Go to Security → List Users
- Find the user (username format: `assumed-role/RoleName`)
- Verify user has ONLY `MWAARestrictedTest` role (no Public role)
- Verify user can see the DAGs page
- Verify user can see only `hello_world_advanced` and `hello_world_simple` DAGs

### 3. Test DAG Visibility

As the custom role user:

**Expected Results:**
- Can access the DAGs page (menu visible)
- Can see `hello_world_advanced` DAG
- Can see `hello_world_simple` DAG
- Cannot see other DAGs (if any exist)
- Can trigger and view runs for allowed DAGs

### 4. Test Role Isolation

Create a second custom role with different DAG permissions and test that users with different roles see different DAGs.

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

2. **Update ALB Configuration:**
   - Deploy the updated `02-alb.yml` template with Airflow 3.x authentication support
   - The default action now forwards to MWAA (not Lambda) to handle `/pluginsv2/*` paths

3. **Update DAGs (Required):**
   - Replace `schedule_interval` with `schedule` in all DAG definitions
   - Remove `provide_context=True` from all PythonOperator instances
   - Replace `DummyOperator` with `EmptyOperator`
   - Remove any `access_control` references to non-existent roles

4. **Deploy Updated Files:**
   ```bash
   # Upload all DAG files with Airflow 3.x compatibility
   ./deploy-stack.sh --vpc-stack mwaa-af3 --upload
   
   # Or manually upload individual DAGs
   aws s3 cp role_creation_dag/update_user_role_dag.py s3://your-bucket/dags/
   aws s3 cp sample_dags/hello_world_simple.py s3://your-bucket/dags/
   aws s3 cp sample_dags/hello_world_advanced.py s3://your-bucket/dags/
   ```

5. **Test Role Creation:**
   - The role creation DAG should work without changes
   - Test with a non-production role first

6. **Update Custom DAGs:**
   - Review all custom DAGs for deprecated operators and parameters
   - Test thoroughly in a development environment

## Troubleshooting

### Authentication Issues

**Issue:** User gets "Failed to configure user access" error

**Possible Causes:**
1. `update_user_role` DAG failed to complete within 60 seconds
2. Custom role doesn't exist in Airflow
3. Database connection issues

**Solution:**
1. Check CloudWatch logs for the `update_user_role` DAG run
2. Verify the custom role exists (Security → List Roles)
3. Check that `DB_SECRETS` environment variable is set in MWAA
4. Manually trigger the `update_user_role` DAG to test

### Connection Issues

**Issue:** `AirflowException: Neither SQL_ALCHEMY_CONN nor MWAA database environment variables are available`

**Solution:** This error should not occur in MWAA Airflow 3.x as the environment variables are automatically set. If you see this error:
1. Check MWAA environment configuration
2. Verify the environment is running Airflow 3.0.6 or later
3. Check CloudWatch logs for the actual environment variables available

**Issue:** `sqlalchemy.exc.ArgumentError: Could not parse SQLAlchemy URL from string 'airflow-db-not-allowed:///'`

**Solution:** This is expected in Airflow 3.x. The DAG should automatically fall back to using `DB_SECRETS` environment variable. If it doesn't:
1. Verify the DAG code has the fallback logic
2. Check that `DB_SECRETS`, `POSTGRES_HOST` environment variables exist
3. Re-upload the updated `update_user_role_dag.py`

### Role Creation Fails

**Issue:** Glue job fails to create role

**Solutions:**
1. Check Glue job logs in CloudWatch
2. Verify the Glue connection is created successfully
3. Ensure the GlueRoleCreatorRole has necessary permissions
4. Check that the source role exists in MWAA
5. Verify the target role doesn't already exist (delete it first if needed)

### User Can't See DAGs

**Issue:** User has custom role but can't see DAGs page

**Possible Causes:**
1. Role missing `menu access on DAGs` permission
2. User has multiple roles (including Public) causing permission conflicts
3. Role was created with old Glue script that excluded DAG menu permissions

**Solution:**
1. Delete the custom role in Airflow UI (Security → List Roles)
2. Re-upload the updated `create_role_glue_job_dag.py` to S3
3. Re-run the `create_role_glue_job` DAG to recreate the role
4. Verify the role has `menu access on DAGs` permission
5. Login again to trigger role assignment

**Issue:** User has both Public and custom roles

**Solution:**
1. Manually remove the Public role from the user (Security → List Users → Edit)
2. Or trigger the `update_user_role` DAG manually with the user's username
3. The DAG should remove ALL roles and add only the custom role

### REST API Issues

**Issue:** Lambda gets 422 error when triggering DAG

**Cause:** Missing `logical_date` field in request body (required in Airflow 3.x)

**Solution:** Verify Lambda code includes `logical_date`:
```python
request_body = {
    'conf': {'username': username, 'role': role},
    'logical_date': datetime.now(timezone.utc).isoformat()
}
```

**Issue:** Lambda gets 404 error on `/users` endpoint

**Cause:** Endpoint removed in Airflow 3.x

**Solution:** Remove any code that calls the `/users` endpoint. The Lambda should not check user roles before triggering the DAG.

## References

- [AWS MWAA Documentation](https://docs.aws.amazon.com/mwaa/)
- [Integrate Amazon MWAA with Microsoft Entra ID using SAML authentication](https://aws.amazon.com/blogs/big-data/integrate-amazon-mwaa-with-microsoft-entra-id-using-saml-authentication/) - Azure AD integration guide
- [Limit access to Apache Airflow DAGs in Amazon MWAA](https://docs.aws.amazon.com/mwaa/latest/userguide/limit-access-to-dags.html) - Custom role creation tutorial
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [MWAA Airflow 3.x Migration Guide](https://docs.aws.amazon.com/mwaa/latest/userguide/airflow-versions.html)
- [Airflow 3.0 Release Notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html)
