# AWS MWAA Custom RBAC Solution

A comprehensive solution for implementing custom Role-Based Access Control (RBAC) in Amazon Managed Workflows for Apache Airflow (MWAA) using AWS Cognito, Application Load Balancer (ALB), and Lambda authorizers.

> **Note**: This solution is fully automated using CloudFormation templates. All components including Lambda functions, DAGs, and IAM policies are deployed and configured automatically without requiring manual deployment scripts.

## Overview

This solution provides:
- **Custom RBAC Integration**: Seamless integration with Azure AD/Cognito for user authentication
- **Dynamic Role Management**: Automated creation and assignment of custom MWAA roles
- **Secure Access Control**: Fine-grained permissions based on user groups
- **Infrastructure as Code**: Complete CloudFormation templates for reproducible deployments
- **Fully Automated Deployment**: No manual scripts required - everything deployed via CloudFormation

## Architecture

### Components

1. **MWAA Environment**: Core Apache Airflow environment with custom execution roles
2. **Application Load Balancer**: HTTPS endpoint with Cognito authentication
3. **Lambda Authorizer**: Handles user authentication and role assignment
4. **Glue Jobs**: Automated role creation and user management
5. **VPC Endpoints**: Secure communication within private subnets

### Authentication Flow

1. User accesses ALB endpoint
2. ALB redirects to Cognito for authentication
3. Lambda authorizer processes JWT token and group memberships
4. Custom MWAA role is assigned based on user groups
5. User is redirected to MWAA UI with appropriate permissions

## Files Overview

### Infrastructure Templates
- `01-vpc-mwaa.yml` - VPC, MWAA environment, and networking components
- `02-alb.yml` - Application Load Balancer and authentication setup

### Lambda Functions
- `lambda_auth/` - Lambda authorizer for authentication
  - `lambda_mwaa-authorizer.py` - Main authorizer function for user authentication

### Airflow DAGs
- `role_creation_dag/` - Custom role creation and management DAGs
  - `create_role_glue_job_dag.py` - Automated custom role creation using Glue jobs
  - `update_user_role_dag.py` - User role assignment and management
- `sample_dags/` - Example DAG files for testing and validation
  - `hello_world_simple.py` - Basic test DAG for role validation
  - `hello_world_advanced.py` - Advanced test DAG with multiple tasks

> **Note**: The sample DAGs in `sample_dags/` are automatically deployed to MWAA by the CloudFormation template. They are embedded inline in the `02-alb.yml` template and uploaded to S3 during stack creation. You can use these as templates for creating your own DAGs.

> **Note**: The `create_role_glue_job_dag.py` in `role_creation_dag/` is too large to embed in CloudFormation (14KB+) and is uploaded separately by the `deploy-stack.sh` script during the file upload step.

> **Note**: The `update_user_role_dag.py` is embedded in the CloudFormation template and automatically deployed to S3. The source file in `role_creation_dag/` is kept for reference and version control.

> **Note**: The Lambda authorizer code in `lambda_auth/` is uploaded to S3 by the `deploy-stack.sh` script and automatically deployed by CloudFormation custom resources.

### Deployment Scripts
- `deploy-stack.sh` - Main deployment orchestration script with individual step flags
- `cleanup-stack.sh` - Stack cleanup script with individual step flags

### Configuration Files
- `deployment-config.json` - Deployment parameters
- `deployment-config.template.json` - Template for deployment configuration

## Quick Start

### Prerequisites

- AWS CLI configured with appropriate permissions
- Python 3.8+ for local development
- Azure AD tenant (for SSO integration)
- `jq` command-line JSON processor
- Bash shell (for deployment script)

### Quick Reference

**Deployment:**
```bash
# Full deployment (all steps with default stack names)
./deploy-stack.sh

# Full deployment with custom stack names
./deploy-stack.sh --vpc-stack my-mwaa-vpc --alb-stack my-mwaa-alb

# Individual steps
./deploy-stack.sh --vpc              # Deploy VPC and MWAA
./deploy-stack.sh --upload           # Upload files to S3
./deploy-stack.sh --alb              # Deploy ALB and auth

# Individual steps with custom stack names
./deploy-stack.sh --vpc-stack my-vpc --vpc
./deploy-stack.sh --vpc-stack my-vpc --upload
./deploy-stack.sh --vpc-stack my-vpc --alb-stack my-alb --alb

# Combined steps
./deploy-stack.sh --vpc --upload     # Deploy VPC and upload files

# Help
./deploy-stack.sh --help             # Show usage information
```

**Cleanup:**
```bash
# Full cleanup (all steps with confirmations, default stack names)
./cleanup-stack.sh

# Full cleanup with custom stack names
./cleanup-stack.sh --vpc-stack my-vpc --alb-stack my-alb

# Individual steps
./cleanup-stack.sh --alb             # Delete ALB stack only
./cleanup-stack.sh --s3              # Clean S3 bucket only
./cleanup-stack.sh --vpc             # Delete VPC stack only

# Individual steps with custom stack names
./cleanup-stack.sh --vpc-stack my-vpc --alb-stack my-alb --alb
./cleanup-stack.sh --vpc-stack my-vpc --s3
./cleanup-stack.sh --vpc-stack my-vpc --vpc

# Combined steps
./cleanup-stack.sh --alb --s3        # Delete ALB and clean S3

# Skip confirmations
./cleanup-stack.sh --force           # Full cleanup without prompts
./cleanup-stack.sh --alb --force     # Delete ALB without confirmation

# Help
./cleanup-stack.sh --help            # Show usage information
```

**Stack Name Options:**
- `--vpc-stack NAME` - Specify VPC stack name (default: `mwaa-vpc`)
- `--alb-stack NAME` - Specify ALB stack name (default: `<vpc-stack>-alb`)

### Deployment Steps

1. **Configure deployment parameters**
   ```bash
   cp deployment-config.template.json deployment-config.json
   # Edit deployment-config.json with your values
   ```

2. **Run the deployment script**
   ```bash
   ./deploy-stack.sh
   ```

   **Advanced Usage - Run Individual Steps:**
   ```bash
   # Deploy VPC stack only
   ./deploy-stack.sh --vpc
   
   # Upload files to S3 only (requires VPC stack to be deployed)
   ./deploy-stack.sh --upload
   
   # Deploy ALB stack only (requires VPC stack and files uploaded)
   ./deploy-stack.sh --alb
   
   # Combine steps
   ./deploy-stack.sh --vpc --upload
   
   # Show help
   ./deploy-stack.sh --help
   ```

The deployment script will:
- ✅ Validate prerequisites and required files
- ✅ Deploy the VPC and MWAA infrastructure stack
- ✅ Wait for the VPC stack to complete
- ✅ Extract the S3 bucket name from stack outputs
- ✅ Upload the Lambda function code (lambda_auth/lambda_mwaa-authorizer.py) to S3
- ✅ Upload the role_creation_dag/create_role_glue_job_dag.py to S3
- ✅ Deploy the ALB and authentication stack
- ✅ Display deployment summary with access URLs

3. **Configure Lambda Environment Variables**

   After deployment, you must update the Lambda function's environment variables with your specific configuration:

   ```bash
   # Get the Lambda function name from the ALB stack
   LAMBDA_FUNCTION_NAME=$(aws cloudformation describe-stacks \
     --stack-name mwaa-vpc-alb \
     --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionName`].OutputValue' \
     --output text)
   
   # Get the MWAA environment name from the VPC stack
   MWAA_ENV_NAME=$(aws cloudformation describe-stacks \
     --stack-name mwaa-vpc \
     --query 'Stacks[0].Outputs[?OutputKey==`MWAAEnvironmentName`].OutputValue' \
     --output text)
   
   # Get your AWS Account ID
   AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
   
   # Get your current AWS region
   AWS_REGION=$(aws configure get region)
   
   # Update the Lambda function environment variables
   aws lambda update-function-configuration \
     --function-name $LAMBDA_FUNCTION_NAME \
     --environment Variables='{
       "Amazon_MWAA_ENV_NAME": "'$MWAA_ENV_NAME'",
       "AWS_ACCOUNT_ID": "'$AWS_ACCOUNT_ID'",
       "COGNITO_CLIENT_ID": "auto-configured-by-cloudformation",
       "COGNITO_DOMAIN": "auto-configured-by-cloudformation",
       "AWS_REGION": "'$AWS_REGION'",
       "GROUP_TO_ROLE_MAP": "[{\"idp-group\":\"018b2f48-xxxx-xxxx-xxxx-4cef1b43ff3e\",\"iam-role\":\"mwaa-alb3-MwaaAdminRole-VihdtRX9HGij\",\"mwaa-role\":\"Admin\"},{\"idp-group\":\"737f6c94-xxxx-xxxx-xxxx-6b50db58f558\",\"iam-role\":\"mwaa-alb3-MwaaUserRole-LIYPVmwGlb4c\",\"mwaa-role\":\"User\"},{\"idp-group\":\"8c330ba0-xxxx-xxxx-xxxx-3e7bb29d9741\",\"iam-role\":\"mwaa-alb3-MwaaViewerRole-CTlZ163fDsp6\",\"mwaa-role\":\"Viewer\"},{\"idp-group\":\"a3255ab9-xxxx-xxxx-xxxx-baafac5fc071\",\"iam-role\":\"mwaa-alb3-MwaaCustomRole-BLbt7H2IuzX4\",\"mwaa-role\":\"Restricted\"}]",
       "ALB_COOKIE_NAME": "AWSELBAuthSessionCookie",
       "IDP_LOGIN_URI": "https://login.microsoftonline.com/your-tenant-id/saml2"
     }'
   ```

   **Important**: Replace the following values with your actual configuration:
   - `018b2f48-xxxx-xxxx-xxxx-4cef1b43ff3e` - Your Azure AD admin group UUID (unmask the x's)
   - `737f6c94-xxxx-xxxx-xxxx-6b50db58f558` - Your Azure AD user group UUID (unmask the x's)
   - `8c330ba0-xxxx-xxxx-xxxx-3e7bb29d9741` - Your Azure AD viewer group UUID (unmask the x's)
   - `a3255ab9-xxxx-xxxx-xxxx-baafac5fc071` - Your Azure AD custom group UUID (unmask the x's)
   - `mwaa-alb3-MwaaAdminRole-VihdtRX9HGij` - Actual IAM role names from your CloudFormation stack
   - `https://login.microsoftonline.com/your-tenant-id/saml2` - Your Azure AD SAML login URL

   **To get Azure AD group UUIDs:**
   ```bash
   # Using Azure CLI (if installed)
   az ad group list --display-name "Your Group Name" --query '[].id' --output tsv
   
   # Or check in Azure Portal:
   # 1. Go to Azure Active Directory > Groups
   # 2. Click on your group name
   # 3. Copy the Object ID (this is the UUID you need)
   ```

   **To get the actual IAM role names:**
   ```bash
   # Get IAM role ARNs from ALB stack outputs
   aws cloudformation describe-stacks \
     --stack-name mwaa-vpc-alb \
     --query 'Stacks[0].Outputs[?contains(OutputKey, `Role`)].{Key:OutputKey,Value:OutputValue}' \
     --output table
   ```

   **Alternative: Update environment variables one by one**
   ```bash
   # Update MWAA environment name
   aws lambda update-function-configuration \
     --function-name $LAMBDA_FUNCTION_NAME \
     --environment Variables='{
       "Amazon_MWAA_ENV_NAME": "'$MWAA_ENV_NAME'"
     }'
   
   # Update GROUP_TO_ROLE_MAP with your specific group mappings
   aws lambda update-function-configuration \
     --function-name $LAMBDA_FUNCTION_NAME \
     --environment Variables='{
       "GROUP_TO_ROLE_MAP": "[{\"idp-group\":\"018b2f48-edd5-49fb-9541-4cef1b43ff3e\",\"iam-role\":\"mwaa-alb3-MwaaAdminRole-VihdtRX9HGij\",\"mwaa-role\":\"Admin\"},{\"idp-group\":\"737f6c94-bf1d-4079-a726-6b50db58f558\",\"iam-role\":\"mwaa-alb3-MwaaUserRole-LIYPVmwGlb4c\",\"mwaa-role\":\"User\"}]"
     }'
   
   # Update Azure AD login URL
   aws lambda update-function-configuration \
     --function-name $LAMBDA_FUNCTION_NAME \
     --environment Variables='{
       "IDP_LOGIN_URI": "https://login.microsoftonline.com/your-tenant-id/saml2"
     }'
   ```

4. **Test the Authentication Flow**

   Once the environment variables are updated, you can access the MWAA UI through the ALB endpoint.

## Deployment Steps Explained

The deployment script supports running individual steps for more control over the deployment process:

### Step 1: VPC Deployment (`--vpc`)
Deploys the VPC, MWAA environment, and all networking components. This step takes the longest (typically 20-30 minutes for MWAA environment creation).

**When to use:**
- Initial deployment
- Updating VPC or MWAA configuration
- Troubleshooting network issues

```bash
./deploy-stack.sh --vpc
```

### Step 2: File Upload (`--upload`)
Uploads Lambda function code and DAG files to the S3 bucket created by the VPC stack.

**When to use:**
- After VPC deployment completes
- Updating Lambda function code
- Updating DAG files
- Re-uploading files after changes

```bash
./deploy-stack.sh --upload
```

### Step 3: ALB Deployment (`--alb`)
Deploys the Application Load Balancer, Lambda authorizer, and authentication components.

**When to use:**
- After VPC deployment and file upload
- Updating ALB or authentication configuration
- Updating Lambda environment variables via CloudFormation

```bash
./deploy-stack.sh --alb
```

### Combined Steps
You can combine steps for partial deployments:

```bash
# Deploy VPC and upload files (useful for initial setup)
./deploy-stack.sh --vpc --upload

# Upload files and deploy ALB (useful for updates)
./deploy-stack.sh --upload --alb
```

## Cleanup Steps Explained

The cleanup script supports running individual steps for controlled teardown of resources:

### Step 1: ALB Deletion (`--alb`)
Deletes the Application Load Balancer stack including Lambda authorizer, Cognito resources, and authentication components.

**When to use:**
- Removing authentication layer while keeping MWAA environment
- Updating ALB configuration (delete then redeploy)
- Troubleshooting ALB issues
- Reducing costs by removing ALB temporarily

```bash
./cleanup-stack.sh --alb
```

**What gets deleted:**
- Application Load Balancer and target groups
- Lambda authorizer function
- Cognito User Pool and Identity Provider
- ALB security groups and listeners
- CloudWatch log groups for Lambda

### Step 2: S3 Bucket Cleanup (`--s3`)
Empties the S3 bucket by removing all objects, versions, and delete markers. The bucket itself is deleted when the VPC stack is removed.

**When to use:**
- Before deleting VPC stack (required)
- Cleaning up old DAG files and logs
- Removing uploaded Lambda code
- Preparing for fresh deployment

```bash
./cleanup-stack.sh --s3
```

**What gets deleted:**
- All DAG files
- Lambda function code
- MWAA logs and artifacts
- All object versions (if versioning enabled)
- Delete markers

**Important:** This step requires confirmation unless `--force` flag is used. The script shows object count before deletion.

### Step 3: VPC Deletion (`--vpc`)
Deletes the VPC stack including MWAA environment, networking components, and all associated resources.

**When to use:**
- Complete teardown of infrastructure
- After ALB and S3 cleanup are complete
- Removing MWAA environment permanently

```bash
./cleanup-stack.sh --vpc
```

**What gets deleted:**
- MWAA environment (takes 20-30 minutes)
- VPC and all subnets
- VPC endpoints (S3, CloudWatch, Glue, etc.)
- Security groups
- NAT gateways and Elastic IPs
- S3 bucket (must be empty first)
- IAM roles (MWAA execution role, Glue role)
- CloudWatch log groups

**Important:** This step requires typing "yes" to confirm (not just "y") unless `--force` flag is used. This operation cannot be undone.

### Force Mode (`--force`)
Skips all confirmation prompts for automated cleanup workflows.

**When to use:**
- Automated CI/CD pipelines
- Scripted teardown processes
- When you're certain about deletion

```bash
# Full cleanup without any prompts
./cleanup-stack.sh --force

# Delete specific resources without prompts
./cleanup-stack.sh --alb --s3 --force
```

**Warning:** Use with caution - this will delete resources immediately without confirmation.

### Cleanup Workflow Examples

**Full Cleanup (Recommended):**
```bash
# Run all steps with confirmations
./cleanup-stack.sh

# Or without confirmations
./cleanup-stack.sh --force
```

**Step-by-Step Cleanup:**
```bash
# Step 1: Delete ALB stack
./cleanup-stack.sh --alb

# Step 2: Clean S3 bucket (after ALB deletion completes)
./cleanup-stack.sh --s3

# Step 3: Delete VPC stack (after S3 is empty)
./cleanup-stack.sh --vpc
```

**Partial Cleanup (Keep MWAA, Remove ALB):**
```bash
# Remove ALB only, keep MWAA environment running
./cleanup-stack.sh --alb
```

**Quick Cleanup (ALB + S3):**
```bash
# Delete ALB and clean S3 in one command
./cleanup-stack.sh --alb --s3 --force
```

### Cleanup Validation

After cleanup, verify resources are deleted:

```bash
# Check CloudFormation stacks
aws cloudformation list-stacks --stack-status-filter DELETE_COMPLETE

# Verify S3 bucket is empty (before VPC deletion)
aws s3 ls s3://your-bucket-name --recursive

# Check if MWAA environment is deleted
aws mwaa list-environments

# Verify VPC is deleted
aws ec2 describe-vpcs --filters "Name=tag:Name,Values=*mwaa*"
```

### Troubleshooting Cleanup Issues

**ALB Stack Deletion Fails:**
```bash
# Check for resources preventing deletion
aws cloudformation describe-stack-events \
  --stack-name mwaa-vpc-alb \
  --query 'StackEvents[?ResourceStatus==`DELETE_FAILED`]'

# Common issue: Lambda function still has ENIs attached
# Wait a few minutes and retry
./cleanup-stack.sh --alb
```

**S3 Bucket Not Empty:**
```bash
# Force empty the bucket
aws s3 rm s3://your-bucket-name --recursive

# Delete all versions
aws s3api list-object-versions \
  --bucket your-bucket-name \
  --query 'Versions[].{Key:Key,VersionId:VersionId}' \
  --output json | \
jq -r '.[] | "--key \"\(.Key)\" --version-id \"\(.VersionId)\""' | \
xargs -I {} aws s3api delete-object --bucket your-bucket-name {}
```

**VPC Stack Deletion Fails:**
```bash
# Check for dependencies
aws cloudformation describe-stack-events \
  --stack-name mwaa-vpc \
  --query 'StackEvents[?ResourceStatus==`DELETE_FAILED`]'

# Common issues:
# 1. S3 bucket not empty - run cleanup-stack.sh --s3
# 2. ENIs still attached - wait 5-10 minutes
# 3. MWAA environment deletion in progress - wait for completion
```

**MWAA Environment Stuck Deleting:**
```bash
# Check MWAA environment status
aws mwaa get-environment --name your-environment-name

# If stuck for >1 hour, contact AWS Support
# MWAA deletion typically takes 20-30 minutes
```

### Manual Deployment (Alternative)

If you prefer to deploy manually:

1. **Deploy VPC stack**
   ```bash
   aws cloudformation create-stack \
     --stack-name mwaa-vpc \
     --template-body file://01-vpc-mwaa.yml \
     --parameters file://deployment-config.json \
     --capabilities CAPABILITY_NAMED_IAM
   ```

2. **Wait for VPC stack completion and upload files**
   ```bash
   # Get S3 bucket name from stack outputs
   BUCKET_NAME=$(aws cloudformation describe-stacks \
     --stack-name mwaa-vpc \
     --query 'Stacks[0].Outputs[?OutputKey==`MwaaS3BucketName`].OutputValue' \
     --output text)
   
   # Upload Lambda function code
   aws s3 cp lambda_auth/lambda_mwaa-authorizer.py s3://$BUCKET_NAME/lambda-code/lambda_mwaa-authorizer.py
   
   # Upload create_role_glue_job_dag.py
   aws s3 cp role_creation_dag/create_role_glue_job_dag.py s3://$BUCKET_NAME/dags/create_role_glue_job_dag.py
   ```

3. **Deploy ALB stack**
   ```bash
   # Add MWAAVPCStackName parameter to deployment config
   jq '. + [{"ParameterKey": "MWAAVPCStackName", "ParameterValue": "mwaa-vpc"}]' \
     deployment-config.json > alb-config.json
   
   aws cloudformation create-stack \
     --stack-name mwaa-vpc-alb \
     --template-body file://02-alb.yml \
     --parameters file://alb-config.json \
     --capabilities CAPABILITY_NAMED_IAM
   ```

## Configuration

### Deployment Configuration

Edit `deployment-config.json` with your specific values:

```json
[
  {
    "ParameterKey": "VpcCIDR",
    "ParameterValue": "10.192.0.0/16"
  },
  {
    "ParameterKey": "MWAAEnvironmentName",
    "ParameterValue": "mwaa-v3"
  },
  {
    "ParameterKey": "ALBCertificateArn",
    "ParameterValue": "arn:aws:acm:us-east-1:123456789012:certificate/your-certificate-id"
  },
  {
    "ParameterKey": "AzureAdminGroupID",
    "ParameterValue": "your-azure-admin-group-uuid"
  },
  {
    "ParameterKey": "AzureUserGroupID",
    "ParameterValue": "your-azure-user-group-uuid"
  },
  {
    "ParameterKey": "AzureViewerGroupID",
    "ParameterValue": "your-azure-viewer-group-uuid"
  },
  {
    "ParameterKey": "AzureCustomGroupID",
    "ParameterValue": "your-azure-custom-group-uuid"
  },
  {
    "ParameterKey": "EntraIDLoginURL",
    "ParameterValue": "https://login.microsoftonline.com/your-tenant-id/saml2"
  },
  {
    "ParameterKey": "AppFederationMetadataURL",
    "ParameterValue": "https://login.microsoftonline.com/your-tenant-id/federationmetadata/2007-06/federationmetadata.xml"
  }
]
```

**Required Parameters:**
- `ALBCertificateArn` - ACM certificate ARN for HTTPS on ALB (must be in the same region)
- `AzureAdminGroupID` - Azure AD group UUID for admin users
- `AzureUserGroupID` - Azure AD group UUID for standard users
- `AzureViewerGroupID` - Azure AD group UUID for viewer users
- `AzureCustomGroupID` - Azure AD group UUID for custom role users (optional)
- `EntraIDLoginURL` - Azure AD SAML login URL
- `AppFederationMetadataURL` - Azure AD federation metadata URL

**How to get required values:**

1. **ACM Certificate ARN:**
   ```bash
   # List certificates in your region
   aws acm list-certificates --region us-east-1
   
   # Or request a new certificate
   aws acm request-certificate \
     --domain-name your-domain.com \
     --validation-method DNS \
     --region us-east-1
   ```

2. **Azure AD Group UUIDs:**
   ```bash
   # Using Azure CLI
   az ad group list --display-name "Your Group Name" --query '[].id' --output tsv
   
   # Or in Azure Portal: Azure Active Directory > Groups > [Group Name] > Object ID
   ```

3. **Azure AD URLs:**
   - **EntraIDLoginURL**: `https://login.microsoftonline.com/{tenant-id}/saml2`
   - **AppFederationMetadataURL**: `https://login.microsoftonline.com/{tenant-id}/federationmetadata/2007-06/federationmetadata.xml`
   - Replace `{tenant-id}` with your Azure AD tenant ID

**Optional Parameters:**
- `VpcCIDR` - VPC CIDR block (default: 10.192.0.0/16)
- `PrivateSubnet1CIDR` - Private subnet 1 CIDR (default: 10.192.10.0/24)
- `PrivateSubnet2CIDR` - Private subnet 2 CIDR (default: 10.192.11.0/24)
- `PrivateSubnet3CIDR` - Private subnet 3 CIDR (default: 10.192.14.0/24)
- `PublicSubnet1CIDR` - Public subnet 1 CIDR (default: 10.192.20.0/24)
- `PublicSubnet2CIDR` - Public subnet 2 CIDR (default: 10.192.21.0/24)
- `MWAAEnvironmentName` - MWAA environment name (default: mwaa-env)

### Lambda Environment Variables

The Lambda function environment variables are partially configured by the CloudFormation template:

**Automatically Configured:**
- `AWS_ACCOUNT_ID` - Your AWS account ID
- `COGNITO_CLIENT_ID` - Cognito User Pool Client ID
- `COGNITO_DOMAIN` - Cognito domain name
- `ALB_COOKIE_NAME` - ALB session cookie name

**Requires Manual Configuration:**
- `Amazon_MWAA_ENV_NAME` - Your MWAA environment name (get from VPC stack outputs)
- `GROUP_TO_ROLE_MAP` - Azure AD group to IAM role mappings
- `IDP_LOGIN_URI` - Your Azure AD login URL

### Group to Role Mapping

Configure user group mappings in the `GROUP_TO_ROLE_MAP` environment variable:

```json
[
  {"idp-group": "018b2f48-xxxx-xxxx-xxxx-4cef1b43ff3e", "iam-role": "mwaa-alb3-MwaaAdminRole-VihdtRX9HGij", "mwaa-role": "Admin"},
  {"idp-group": "737f6c94-xxxx-xxxx-xxxx-6b50db58f558", "iam-role": "mwaa-alb3-MwaaUserRole-LIYPVmwGlb4c", "mwaa-role": "User"},
  {"idp-group": "8c330ba0-xxxx-xxxx-xxxx-3e7bb29d9741", "iam-role": "mwaa-alb3-MwaaViewerRole-CTlZ163fDsp6", "mwaa-role": "Viewer"},
  {"idp-group": "a3255ab9-xxxx-xxxx-xxxx-baafac5fc071", "iam-role": "mwaa-alb3-MwaaCustomRole-BLbt7H2IuzX4", "mwaa-role": "Restricted"}
]
```

**CloudFormation vs. User Mapping:**
- **CloudFormation Template**: MwaaCustomRole is configured to create web login tokens for "Public" role (base MWAA role it can assume)
- **GROUP_TO_ROLE_MAP**: Users in the custom group are mapped to "Restricted" role (custom role you create with limited permissions)
- **Workflow**: MwaaCustomRole assumes "Public" → Lambda triggers DAG → DAG creates/assigns "Restricted" role → User gets "Restricted" permissions

**Note**: 
- `idp-group` values are Azure AD group UUIDs (mask middle sections for security)
- `iam-role` values are the actual CloudFormation-generated IAM role names
- `mwaa-role` values correspond to MWAA roles you want users to have

## Custom Role Creation

**Important**: When creating custom roles, you typically create a "Restricted" role (or other custom role name) that has limited permissions. In your GROUP_TO_ROLE_MAP, you map users to this "Restricted" role, even though the CloudFormation MwaaCustomRole is configured to assume the "Public" role as its base permission.

### Using the Glue Job DAG

The `create_role_glue_job_dag` automates custom role creation:

```python
# Trigger with configuration
{
  "aws_region": "us-east-1",
  "stack_name": "mwaa-v3",
  "source_role": "User",
  "target_role": "CustomRestrictedRole",
  "specific_dags": ["hello_world_advanced", "hello_world_simple"]
}
```

### DAG Workflow

1. **create_glue_connection** - Creates connection to MWAA metadata database
2. **get_glue_role_arn** - Retrieves Glue role ARN from CloudFormation stack
3. **get_s3_bucket_name** - Gets S3 bucket name from CloudFormation stack
4. **upload_glue_script** - Uploads Glue job script to S3
5. **run_glue_job_task** - Creates and executes Glue job for role creation

### Manual Role Creation

1. **Access MWAA UI as Admin**
2. **Navigate to Security > List Roles**
3. **Create new role with desired permissions**
4. **Update group mapping configuration**

## Security Features

### IAM Permissions

The solution includes comprehensive IAM policies:

#### MWAA Execution Role Permissions
- **Environment Access**: `airflow:GetEnvironment`, `airflow:CreateWebLoginToken`, `airflow:CreateCliToken`
- **REST API Access**: `airflow:InvokeRestApi` for all standard roles
- **CloudFormation Access**: `cloudformation:DescribeStacks`, `cloudformation:ListStackOutputs`
- **S3 Access**: Full access to MWAA S3 bucket
- **Glue Access**: Complete Glue job management permissions

#### Custom Role Permissions
- **Web Login Token Creation**: For Public role access
- **Environment Access**: Full MWAA environment permissions
- **REST API Access**: Invoke REST APIs for all roles
- **Admin Token Creation**: Create web login tokens for Admin role

### Network Security

- **VPC Endpoints**: Secure communication to AWS services including:
  - S3 Gateway Endpoint
  - CloudWatch Logs Interface Endpoint
  - CloudFormation Interface Endpoint
  - ECR Interface Endpoints
  - SQS Interface Endpoint
  - KMS Interface Endpoint
  - Airflow Interface Endpoints
  - STS Interface Endpoint
  - Glue Interface Endpoint
  - EC2 Interface Endpoint

- **Security Groups**: Restricted ingress/egress rules
- **Private Subnets**: MWAA workers isolated from internet

### Authentication

- **JWT Validation**: Secure token verification
- **Group-Based Access**: Role assignment based on AD groups
- **Session Management**: Automatic token refresh and expiration
- **Username Extraction**: Handles long usernames by extracting email addresses

## Monitoring and Debugging

### CloudWatch Logs

Monitor these log groups:
- `/aws/lambda/your-lambda-function`
- `airflow-your-environment-Task`
- `airflow-your-environment-Scheduler`
- `airflow-your-environment-WebServer`

### Common Issues

1. **Session Name Too Long**
   - Error: `ValidationError: Member must have length less than or equal to 64`
   - Solution: Lambda extracts email from username automatically

2. **Access Denied to REST API**
   - Error: `No Airflow role granted in IAM`
   - Solution: Ensure roles have `airflow:InvokeRestApi` permission

3. **VPC Connectivity Issues**
   - Error: Connection timeouts
   - Solution: Verify VPC endpoints and security group rules

4. **Glue Job Failures**
   - Error: S3 access denied
   - Solution: Check Glue role permissions and VPC endpoint policies

### Debugging Commands

See the **Deployment Validation** section above for comprehensive verification commands.

## Infrastructure Components

### CloudFormation Templates

#### VPC Template (`01-vpc-mwaa.yml`)
- **VPC Configuration**: Complete VPC setup with public/private subnets
- **MWAA Environment**: Fully configured Airflow environment
- **VPC Endpoints**: Secure communication endpoints for AWS services
- **IAM Roles**: MWAA execution role and Glue role creator
- **Security Groups**: Network access controls
- **S3 Bucket**: Dedicated bucket for DAGs and artifacts

#### ALB Template (`02-alb.yml`)
- **Application Load Balancer**: Internet-facing ALB with HTTPS
- **Cognito Integration**: User pool and identity provider setup
- **Lambda Authorizer**: Function for user authentication and role assignment
- **Target Groups**: ALB routing to MWAA and Lambda
- **Custom Resources**: Automated DAG and Lambda code deployment

> **Note**: The ALB template expects the Lambda function code (lambda_auth/lambda_mwaa-authorizer.py) and role_creation_dag/create_role_glue_job_dag.py to be uploaded to S3 before deployment. The deploy-stack.sh script handles this automatically.

### Automated Deployment Features

The deployment script (`deploy-stack.sh`) provides:
- **Orchestrated Deployment**: Proper sequencing of VPC → File Upload → ALB deployment
- **Error Handling**: Comprehensive error checking and rollback on failure
- **Progress Monitoring**: Real-time status updates and stack completion waiting
- **Automatic File Upload**: Uploads Lambda code and DAGs to the correct S3 locations
- **Configuration Validation**: Checks for required files and dependencies
- **Deployment Summary**: Displays access URLs and next steps after completion

The CloudFormation templates include:
- **Automatic DAG Deployment**: Custom resource deploys standard DAGs to S3
- **Lambda Code Deployment**: Custom resource packages and deploys Lambda function
- **Environment Variable Configuration**: Automatic setup of all required variables
- **IAM Policy Management**: Complete policy definitions for all components
- **VPC Endpoint Creation**: All required endpoints for secure communication

## Best Practices

### Security

- Use least privilege IAM policies
- Enable VPC Flow Logs for network monitoring
- Regularly rotate Cognito client secrets
- Monitor CloudTrail for API access patterns
- Implement resource-based policies where applicable

### Performance

- Right-size MWAA environment class
- Use appropriate worker scaling settings
- Monitor DAG performance metrics
- Optimize database queries in custom roles
- Use efficient Glue job configurations

### Maintenance

- Regular backup of MWAA metadata
- Monitor CloudWatch alarms
- Keep Lambda functions updated
- Review and update IAM policies
- Test role creation workflows regularly

## Deployment Validation

After deployment, verify:

1. **MWAA Environment**: Check environment status is "AVAILABLE"
   ```bash
   aws mwaa get-environment --name your-environment
   ```

2. **ALB Health**: Verify ALB target health
   ```bash
   aws elbv2 describe-target-health --target-group-arn your-target-group-arn
   ```

3. **Lambda Function**: Test authorizer function
   ```bash
   aws lambda invoke --function-name your-function test-output.json
   ```

4. **VPC Endpoints**: Confirm all endpoints are available
   ```bash
   aws ec2 describe-vpc-endpoints --vpc-endpoint-ids vpce-xxxxx
   ```

5. **IAM Roles**: Validate role policies and trust relationships
   ```bash
   aws iam get-role-policy --role-name your-role --policy-name your-policy
   ```

6. **S3 Bucket**: Ensure DAGs are uploaded and accessible
   ```bash
   aws s3 ls s3://your-mwaa-bucket/dags/
   ```

## Troubleshooting Guide

### Permission Issues

1. **Lambda Environment Variable Configuration**
   ```bash
   # Check current Lambda environment variables
   aws lambda get-function-configuration --function-name your-function-name --query 'Environment.Variables'
   
   # Verify MWAA environment name is correct
   aws mwaa list-environments --query 'Environments[0]'
   
   # Test Lambda function with sample event
   aws lambda invoke --function-name your-function-name --payload '{}' test-output.json
   ```

2. **MWAA Access Denied**
   ```bash
   # Check execution role permissions
   aws iam simulate-principal-policy --policy-source-arn arn:aws:iam::account:role/role-name --action-names airflow:CreateWebLoginToken --resource-arns arn:aws:airflow:region:account:role/env/role
   ```

3. **Lambda Execution Errors**
   ```bash
   # Check Lambda logs
   aws logs filter-log-events --log-group-name /aws/lambda/function-name --start-time $(date -d '1 hour ago' +%s)000
   ```

4. **Glue Job Failures**
   ```bash
   # Check Glue job run details
   aws glue get-job-run --job-name job-name --run-id run-id
   ```

### Network Connectivity

1. **VPC Endpoint Issues**
   ```bash
   # Check endpoint status
   aws ec2 describe-vpc-endpoints --filters Name=vpc-id,Values=vpc-xxxxx
   ```

2. **Security Group Rules**
   ```bash
   # Verify security group rules
   aws ec2 describe-security-groups --group-ids sg-xxxxx
   ```

3. **Route Table Configuration**
   ```bash
   # Check route tables
   aws ec2 describe-route-tables --filters Name=vpc-id,Values=vpc-xxxxx
   ```

## Support and Maintenance

### Regular Tasks

- Monitor CloudWatch metrics and alarms
- Review and rotate access keys/secrets
- Update Lambda function code as needed
- Backup MWAA metadata database
- Test disaster recovery procedures

### Updates and Patches

- Keep CloudFormation templates updated
- Update Lambda runtime versions
- Apply security patches to custom code
- Review and update IAM policies
- Monitor AWS service updates

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## References

- [AWS MWAA Documentation](https://docs.aws.amazon.com/mwaa/)
- [Integrate Amazon MWAA with Microsoft Entra ID using SAML authentication](https://aws.amazon.com/blogs/big-data/integrate-amazon-mwaa-with-microsoft-entra-id-using-saml-authentication/) - Azure AD integration guide
- [Limit access to Apache Airflow DAGs in Amazon MWAA](https://docs.aws.amazon.com/mwaa/latest/userguide/limit-access-to-dags.html) - Custom role creation tutorial
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [AWS Lambda Documentation](https://docs.aws.amazon.com/lambda/)
- [AWS CloudFormation Documentation](https://docs.aws.amazon.com/cloudformation/)

## Changelog

### v2.0.0 (Current)
- **Complete CloudFormation Automation**: All components deployed via CloudFormation
- **Orchestrated Deployment Script**: deploy-stack.sh handles proper deployment sequencing
- **Automated File Upload**: Script uploads Lambda code and DAGs to S3 automatically
- **Automated DAG Deployment**: Custom resources handle standard DAG uploads
- **Automated Lambda Deployment**: Custom resources package and deploy Lambda code
- **Comprehensive MWAA Permissions**: Full IAM policy integration
- **Enhanced VPC Endpoint Configuration**: All required endpoints included
- **Dynamic Resource Discovery**: CloudFormation stack outputs integration
- **Improved Error Handling**: Enhanced Lambda authorizer with better logging
- **Streamlined Deployment**: Single script deployment with progress monitoring

### v1.0.0
- Initial release with basic RBAC functionality
- Cognito integration
- Lambda authorizer implementation
- CloudFormation templates