# AWS MWAA Custom RBAC Solution

A comprehensive solution for implementing custom Role-Based Access Control (RBAC) in Amazon Managed Workflows for Apache Airflow (MWAA) using CloudFormation, Lambda authorizers, and Azure AD integration.

## Overview

This solution enables fine-grained access control for MWAA environments by:
- Integrating with Azure AD (Entra ID) for authentication
- Using Application Load Balancer (ALB) with Lambda authorizers
- Creating custom MWAA roles with specific DAG permissions
- Supporting both Airflow 2.x and 3.x

## Architecture

```
Azure AD → ALB → Lambda Authorizer → MWAA Environment
                      ↓
              IAM Role Assumption
                      ↓
              MWAA Role Assignment
```

## Features

- **Azure AD Integration**: Authenticate users via Azure AD SAML
- **Custom Role Creation**: Create MWAA roles with specific DAG permissions using AWS Glue
- **Dynamic Role Assignment**: Automatically assign roles based on Azure AD group membership
- **Airflow 2.x & 3.x Support**: Compatible with both major Airflow versions
- **Infrastructure as Code**: Complete CloudFormation templates for reproducible deployments
- **Automated Deployment**: Shell scripts for easy deployment and cleanup

## Directory Structure

```
mwaa-rbac-custom-roles/
├── airflow2/              # Airflow 2.x compatible implementation
│   ├── 01-vpc-mwaa.yml    # VPC and MWAA environment
│   ├── 02-alb.yml         # ALB and authentication
│   ├── deploy-stack.sh    # Deployment script
│   ├── cleanup-stack.sh   # Cleanup script
│   ├── lambda_auth/       # Lambda authorizer code
│   ├── role_creation_dag/ # DAGs for role management
│   └── sample_dags/       # Example DAGs
│
├── airflow3/              # Airflow 3.x compatible implementation
│   ├── 01-vpc-mwaa.yml    # VPC and MWAA environment
│   ├── 02-alb.yml         # ALB and authentication (updated for AF3 auth)
│   ├── deploy-stack.sh    # Deployment script (updated with DAG upload)
│   ├── cleanup-stack.sh   # Cleanup script
│   ├── lambda_auth/       # Lambda authorizer code (updated for AF3 endpoints)
│   ├── role_creation_dag/ # DAGs for role management (updated for AF3)
│   ├── sample_dags/       # Example DAGs (updated for AF3)
│   ├── AIRFLOW3_AUTH_FIX.md  # Detailed AF3 authentication guide
│   └── README.md          # Airflow 3.x specific documentation
│
└── README.md              # This file
```

## Airflow Version Compatibility

### Airflow 2.x (airflow2/)
- Tested with Airflow 2.5.x, 2.6.x, 2.7.x, 2.8.x, 2.9.x, 2.10.x
- Uses standard authentication endpoints
- Compatible with `schedule_interval` and `DummyOperator`

### Airflow 3.x (airflow3/)
- Tested with Airflow 3.0.x
- **Updated authentication**: New `/auth/login/` endpoint and `/pluginsv2/*` paths
- **Updated DAG syntax**: Uses `schedule` instead of `schedule_interval`
- **Updated operators**: Uses `EmptyOperator` instead of `DummyOperator`
- **Removed parameters**: `provide_context=True` no longer needed
- **Database access**: Uses direct PostgreSQL via `psycopg2` (bypasses `airflow-db-not-allowed` restriction)
- **REST API changes**: Requires `logical_date` field when triggering DAGs, `/users` endpoint removed
- **Role management**: Ensures users have exactly ONE role, removes all existing roles before assignment
- **Custom role creation**: Preserves `menu access on DAGs` permission for UI visibility
- See `airflow3/AIRFLOW3_AUTH_FIX.md` for detailed authentication changes
- See `airflow3/README.md` for complete migration guide and troubleshooting

## Prerequisites

- AWS CLI configured with appropriate credentials
- jq (JSON processor)
- Azure AD tenant with SAML application configured
- ACM certificate for ALB HTTPS
- Sufficient AWS service limits (VPCs, MWAA environments, etc.)

## Quick Start

### 1. Configure Deployment Parameters

Copy the template and fill in your values:

```bash
cd airflow2  # or airflow3
cp deployment-config.template.json deployment-config.json
```

Edit `deployment-config.json` with your:
- VPC CIDR ranges
- MWAA environment name
- ALB certificate ARN
- Azure AD group IDs
- Azure AD tenant URLs

### 2. Deploy Infrastructure

**Full deployment:**
```bash
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb
```

**Step-by-step deployment:**
```bash
# Deploy VPC and MWAA
./deploy-stack.sh --vpc-stack my-mwaa --vpc

# Upload files to S3
./deploy-stack.sh --vpc-stack my-mwaa --upload

# Deploy ALB and authentication
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --alb
```

**Update Lambda environment variables only:**
```bash
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --update-lambda
```

### 3. Create Custom Roles

Trigger the `create_role_glue_job` DAG with configuration:

```json
{
  "aws_region": "us-east-1",
  "stack_name": "my-mwaa",
  "source_role": "User",
  "target_role": "Restricted",
  "specific_dags": ["hello_world_advanced", "hello_world_simple"]
}
```

### 4. Access MWAA

Navigate to the ALB URL (from CloudFormation outputs):
```
https://<alb-dns>/aws_mwaa/aws-console-sso
```

## Deployment Options

### Command-Line Flags

- `--vpc` - Deploy VPC stack only
- `--upload` - Upload files to S3 only
- `--alb` - Deploy ALB stack only
- `--update-lambda` - Update Lambda environment variables only
- `--vpc-stack NAME` - Specify VPC stack name (default: mwaa-vpc)
- `--alb-stack NAME` - Specify ALB stack name (default: <vpc-stack>-alb)

### Examples

```bash
# Deploy with custom stack names
./deploy-stack.sh --vpc-stack prod-mwaa --alb-stack prod-mwaa-alb

# Deploy only VPC
./deploy-stack.sh --vpc-stack dev-mwaa --vpc

# Update Lambda configuration after role changes
./deploy-stack.sh --vpc-stack prod-mwaa --alb-stack prod-mwaa-alb --update-lambda
```

## Airflow 2.x vs 3.x

### Key Differences

| Feature | Airflow 2.x | Airflow 3.x |
|---------|-------------|-------------|
| **Authentication Endpoints** | `/login/` | `/auth/login/` |
| **Plugin Paths** | `/aws_mwaa/aws-console-sso` | `/pluginsv2/aws_mwaa/aws-console-sso` (new) |
| **DB Connection Env Var** | `AIRFLOW__CORE__SQL_ALCHEMY_CONN` | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` |
| **MWAA DB Access** | Direct SQL Alchemy | Blocked (`airflow-db-not-allowed`), use `DB_SECRETS` |
| **Database Access Method** | Airflow CLI (`airflow users`) | Direct PostgreSQL via `psycopg2` |
| **REST API - Trigger DAG** | No `logical_date` required | Requires `logical_date` field |
| **REST API - Users Endpoint** | `/users` available | `/users` removed (404) |
| **Schedule Parameter** | `schedule_interval` | `schedule` (required) |
| **Dummy Operator** | `DummyOperator` | `EmptyOperator` (required) |
| **PythonOperator Context** | `provide_context=True` | Always provided (parameter removed) |
| **DAG Definition** | Context manager or `@dag` | Prefer `@dag` decorator |

### Migration Checklist

When migrating from Airflow 2.x to 3.x:

- [ ] Update ALB configuration to handle new authentication endpoints
- [ ] Update Lambda authorizer to include `logical_date` when triggering DAGs
- [ ] Update Lambda authorizer to remove `/users` endpoint dependency
- [ ] Replace `schedule_interval` with `schedule` in all DAGs
- [ ] Remove `provide_context=True` from all PythonOperator instances
- [ ] Replace `DummyOperator` with `EmptyOperator`
- [ ] Update `update_user_role_dag.py` to use direct PostgreSQL access via `psycopg2`
- [ ] Update `create_role_glue_job_dag.py` to preserve `menu access on DAGs` permission
- [ ] Remove `access_control` references to non-existent roles
- [ ] Test authentication flow through ALB
- [ ] Verify role creation DAG works correctly
- [ ] Test that custom role users can see DAGs page
- [ ] Verify users have only ONE role (no Public role)
- [ ] Test all custom DAGs in development environment

See `airflow3/README.md` and `airflow3/AIRFLOW3_AUTH_FIX.md` for detailed migration instructions.

### Backward Compatibility

The Airflow 3.x implementation is backward compatible with Airflow 2.x for database connections. The `create_role_glue_job_dag.py` automatically detects the Airflow version and uses appropriate connection methods.

## Role Mapping

The solution maps Azure AD groups to IAM roles and MWAA roles:

```
Azure AD Group → IAM Role → MWAA Role
─────────────────────────────────────
Admin Group    → MwaaAdminRole    → Admin
User Group     → MwaaUserRole     → User
Viewer Group   → MwaaViewerRole   → Viewer
Custom Group   → MwaaCustomRole   → Restricted (custom)
```

### Standard MWAA Roles

- **Admin**: Full access to all DAGs and Airflow UI
- **Op**: Operational access (trigger, clear, etc.)
- **User**: Can view and trigger DAGs
- **Viewer**: Read-only access
- **Public**: Minimal access

### Custom Roles

Custom roles are created using the `create_role_glue_job` DAG, which:
1. Connects to MWAA metadata database via AWS Glue
2. Copies permissions from a source role (e.g., User)
3. Restricts access to specific DAGs only
4. Creates the new role in the RBAC tables

## Cleanup

To remove all resources:

```bash
# Full cleanup
./cleanup-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb

# Step-by-step cleanup
./cleanup-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --alb
./cleanup-stack.sh --vpc-stack my-mwaa --s3
./cleanup-stack.sh --vpc-stack my-mwaa --vpc

# Force mode (skip confirmations)
./cleanup-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --force
```

## Troubleshooting

### Lambda Authorization Errors

**Error:** `AccessDenied when calling AssumeRole`

**Solution:** Ensure the Lambda execution role has `sts:AssumeRole` and `sts:SetSourceIdentity` permissions for the MWAA IAM roles.

### Role Creation Fails

**Error:** Glue job fails to create role

**Solutions:**
1. Check Glue job logs in CloudWatch
2. Verify Glue connection to MWAA database
3. Ensure source role exists
4. Check GlueRoleCreatorRole permissions
5. Verify target role doesn't already exist (delete it first)

### Custom Role Users Can't See DAGs (Airflow 3.x)

**Error:** User has custom role but DAGs page is empty

**Root Cause:** Role missing `menu access on DAGs` permission

**Solution:**
1. Delete the custom role in Airflow UI (Security → List Roles)
2. Ensure you have the latest `create_role_glue_job_dag.py` (preserves DAG menu access)
3. Re-run the `create_role_glue_job` DAG
4. Verify role has `menu access on DAGs` permission

### Users Have Multiple Roles (Airflow 3.x)

**Error:** User has both Public and custom roles

**Root Cause:** `update_user_role` DAG not removing all existing roles

**Solution:**
1. Ensure you have the latest `update_user_role_dag.py` (removes ALL roles before assignment)
2. Manually remove extra roles from user (Security → List Users → Edit)
3. Or trigger `update_user_role` DAG manually with user's username

### Connection Issues (Airflow 3.x)

**Error:** `sqlalchemy.exc.ArgumentError: Could not parse SQLAlchemy URL from string 'airflow-db-not-allowed:///'`

**Root Cause:** Airflow 3.x blocks direct SQL Alchemy access

**Solution:** This is expected. The DAG should automatically use `DB_SECRETS` environment variable. Verify:
1. DAG code has fallback logic for `DB_SECRETS`
2. Environment variables exist: `DB_SECRETS`, `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`
3. Re-upload the updated `update_user_role_dag.py`

### REST API Errors (Airflow 3.x)

**Error:** 422 error when triggering DAG

**Root Cause:** Missing `logical_date` field (required in Airflow 3.x)

**Solution:** Update Lambda code to include `logical_date`:
```python
request_body = {
    'conf': {'username': username, 'role': role},
    'logical_date': datetime.now(timezone.utc).isoformat()
}
```

**Error:** 404 error on `/users` endpoint

**Root Cause:** Endpoint removed in Airflow 3.x

**Solution:** Remove any code that calls `/users` endpoint. Lambda should not check user roles before triggering DAG.

### Lambda JSON Parsing Error

**Error:** `JSONDecodeError: Expecting property name enclosed in double quotes`

**Solution:** Run the Lambda update command:
```bash
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --update-lambda
```

## Security Considerations

1. **Secrets Management**: Deployment config contains sensitive data - keep it secure
2. **IAM Permissions**: Follow principle of least privilege
3. **Network Security**: MWAA uses private subnets with VPC endpoints
4. **ALB Security**: HTTPS only with valid ACM certificate
5. **Database Access**: Glue jobs use temporary credentials

## Cost Optimization

- Use appropriate MWAA environment size
- Configure auto-scaling for workers
- Use S3 lifecycle policies for logs
- Delete unused Glue connections and jobs
- Monitor CloudWatch logs retention

## Contributing

When contributing:
1. Test changes in both Airflow 2.x and 3.x
2. Update both airflow2/ and airflow3/ folders
3. Update documentation
4. Follow existing code style
5. Test deployment and cleanup scripts

## License

This solution is provided as-is for use with AWS services.

## References

- [AWS MWAA Documentation](https://docs.aws.amazon.com/mwaa/)
- [Integrate Amazon MWAA with Microsoft Entra ID using SAML authentication](https://aws.amazon.com/blogs/big-data/integrate-amazon-mwaa-with-microsoft-entra-id-using-saml-authentication/) - Azure AD integration guide
- [Limit access to Apache Airflow DAGs in Amazon MWAA](https://docs.aws.amazon.com/mwaa/latest/userguide/limit-access-to-dags.html) - Custom role creation tutorial
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Azure AD SAML Configuration](https://docs.microsoft.com/en-us/azure/active-directory/saas-apps/)

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review CloudFormation stack events
3. Check CloudWatch logs for Lambda and Glue jobs
4. Verify Azure AD configuration
