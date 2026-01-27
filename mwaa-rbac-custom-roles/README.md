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
│   ├── 02-alb.yml         # ALB and authentication
│   ├── deploy-stack.sh    # Deployment script
│   ├── cleanup-stack.sh   # Cleanup script
│   ├── lambda_auth/       # Lambda authorizer code
│   ├── role_creation_dag/ # DAGs for role management (updated for AF3)
│   └── sample_dags/       # Example DAGs
│
└── README.md              # This file
```

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
| DB Connection Env Var | `AIRFLOW__CORE__SQL_ALCHEMY_CONN` | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` |
| MWAA DB Access | Direct SQL Alchemy | May use `DB_SECRETS` env var |
| DAG Definition | Context manager or `@dag` | Prefer `@dag` decorator |
| Schedule Parameter | `schedule_interval` | `schedule` |
| Dummy Operator | `DummyOperator` | `EmptyOperator` |

### Backward Compatibility

The Airflow 3.x implementation is backward compatible with Airflow 2.x. The `create_role_glue_job_dag.py` automatically detects the Airflow version and uses appropriate connection methods.

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

### Connection Issues (Airflow 3.x)

**Error:** `Neither SQL_ALCHEMY_CONN nor MWAA database environment variables are available`

**Solution:** The DAG automatically handles both Airflow 2.x and 3.x connection methods. If this error occurs, check MWAA environment configuration.

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
