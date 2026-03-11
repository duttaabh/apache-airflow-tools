# AWS MWAA Custom RBAC Solution

A comprehensive solution for implementing custom Role-Based Access Control (RBAC) in Amazon Managed Workflows for Apache Airflow (MWAA) using CloudFormation, Lambda authorizers, and Azure AD integration.

## Overview

This solution enables fine-grained access control for MWAA environments by:
- Integrating with Azure AD (Entra ID) for authentication
- Using Application Load Balancer (ALB) with Lambda authorizers
- Creating custom MWAA roles with specific DAG permissions
- Supporting both Airflow 2.x and 3.x with direct database access

## Key Features

### V2 (Current - Direct Database Access)
- **Fast Login**: 3-5 seconds (vs 60+ seconds with DAG-based approach)
- **More Reliable**: No dependency on DAG scheduler
- **Simpler Architecture**: Fewer moving parts, easier to maintain
- **Auto-Configuration**: Reads credentials from Glue connection `{env_name}_metadata_conn`
- **User Management**: Automatically sets first_name and last_name to username
- **Platform-Specific Build**: Lambda layer built with Docker for proper psycopg2 binary
- **Proper VPC Configuration**: Lambda in subnet with NAT Gateway and MWAA security group
- **Username Extraction**: Uses `custom:idp-name` from JWT token (fixes 64-char limit)
- **Specific DAG Access**: Supports `specific_dags` array from GROUP_TO_ROLE_MAP environment variable
- **Direct Role Creation**: Creates custom roles directly in database without DAG dependency

### Recent Updates (February 2026)
- ✅ **Azure SSO Automation**: Integrated Azure Enterprise App creation into deploy-stack.sh with `--setup-azure-sso` flag
- ✅ **SAML Certificate Generation**: Auto-generates 3-year valid SAML token signing certificates
- ✅ **SSL Certificate Automation**: Auto-creates self-signed certificates and imports to ACM with `--create-cert` flag
- ✅ **Direct Database Access**: Replaced DAG-based authentication with direct PostgreSQL access via Glue connection
- ✅ **Username Fix**: Extract username from `custom:idp-name` JWT claim (fixes 64-character RoleSessionName limit)
- ✅ **Specific DAG Support**: Pass `specific_dags` from GROUP_TO_ROLE_MAP to create_custom_role() for granular access
- ✅ **Lambda Layer Automation**: Integrated psycopg2 layer build into deploy-stack.sh
- ✅ **VPC Configuration Fix**: Lambda now uses MWAA security group for database access
- ✅ **NAT Gateway Routing**: Lambda in subnet-3 for internet access (ALB public keys)
- ✅ **User Name Population**: Automatically sets first_name and last_name in database
- ✅ **Airflow2 Backport**: All airflow3 improvements backported to airflow2
- ✅ **Consolidated Documentation**: Single README with all information
- ✅ **File Naming**: Renamed v1 files to `*_with_dag.py` for clarity

### Architecture

```
Azure AD → ALB → Lambda Authorizer → MWAA Environment
                      ↓
              IAM Role Assumption
                      ↓
         Direct Database Access (V2)
                      ↓
              MWAA Role Assignment
```

## Directory Structure

```
mwaa-rbac-custom-roles/
├── airflow2/              # Airflow 2.x compatible implementation
│   ├── 01-vpc-mwaa.yml    # VPC and MWAA environment
│   ├── 02-alb.yml         # ALB and authentication
│   ├── deploy-stack.sh    # Deployment script with Lambda layer and Azure SSO support
│   ├── cleanup-stack.sh   # Cleanup script
│   ├── lambda_auth/       # Lambda authorizer code
│   │   ├── lambda_mwaa-authorizer.py                    # V2 - Direct DB (current)
│   │   ├── lambda_mwaa-authorizer_with_dag.py           # V1 - DAG-based (backup)
│   │   ├── lambda_mwaa-authorizer_with_dag_original.py  # Original airflow2 version
│   │   ├── mwaa_database_manager.py                     # Database helper class
│   │   ├── Dockerfile                                   # Lambda layer build
│   │   └── requirements_layer.txt                       # Layer dependencies
│   ├── role_creation_dag/ # DAGs for role management
│   ├── sample_dags/       # Example DAGs
│   └── azure_sso/         # Azure SSO automation scripts
│       ├── create_nongallery_saml_app.py                # Azure Enterprise App creation
│       ├── verify_config.py                             # Verify SAML configuration
│       ├── requirements.txt                             # Python dependencies
│       └── README.md                                    # Azure SSO documentation
│
├── airflow3/              # Airflow 3.x compatible implementation
│   ├── 01-vpc-mwaa.yml    # VPC and MWAA environment
│   ├── 02-alb.yml         # ALB and authentication (updated for AF3)
│   ├── deploy-stack.sh    # Deployment script with Lambda layer and Azure SSO support
│   ├── cleanup-stack.sh   # Cleanup script
│   ├── lambda_auth/       # Lambda authorizer code
│   │   ├── lambda_mwaa-authorizer.py                    # V2 - Direct DB (current)
│   │   ├── lambda_mwaa-authorizer_with_dag.py           # V1 - DAG-based (backup)
│   │   ├── mwaa_database_manager.py                     # Database helper class
│   │   ├── Dockerfile                                   # Lambda layer build
│   │   └── requirements_layer.txt                       # Layer dependencies
│   ├── role_creation_dag/ # DAGs for role management (updated for AF3)
│   ├── sample_dags/       # Example DAGs (updated for AF3)
│   └── azure_sso/         # Azure SSO automation scripts
│       ├── create_nongallery_saml_app.py                # Azure Enterprise App creation
│       ├── verify_config.py                             # Verify SAML configuration
│       ├── requirements.txt                             # Python dependencies
│       └── README.md                                    # Azure SSO documentation
│
└── README.md              # This file
```

## Prerequisites

- AWS CLI configured with appropriate credentials
- Docker (for building Lambda layer)
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
- Azure AD group IDs
- Azure AD tenant URLs (or use `--setup-azure-sso` to auto-configure)

**Note:** ALB certificate ARN is auto-generated if you use `--create-cert` or full deployment.

### 2. Deploy Infrastructure

**Full deployment with Azure SSO automation (recommended):**
```bash
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --setup-azure-sso
```

This automatically:
1. **Creates self-signed SSL certificate and imports to ACM**
2. Deploys VPC and MWAA environment
3. **Creates Azure Enterprise Application for SAML SSO**
4. **Generates 3-year valid SAML token signing certificate**
5. **Retrieves Metadata URL, Login URL, and certificate details from Azure**
6. **Updates deployment-config.json with Azure SSO URLs**
7. Uploads files to S3
8. **Builds and deploys Lambda layer** (psycopg2 for database access)
9. Deploys ALB and authentication with Azure SSO URLs
10. Updates Lambda environment variables

**Full deployment without Azure SSO automation:**
```bash
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb
```

This automatically:
1. Deploys VPC and MWAA environment
2. Uploads files to S3
3. **Builds and deploys Lambda layer** (psycopg2 for database access)
4. Deploys ALB and authentication
5. Updates Lambda environment variables

**Step-by-step deployment:**
```bash
# 1. Deploy VPC and MWAA
./deploy-stack.sh --vpc-stack my-mwaa --vpc

# 2. (Optional) Setup Azure SSO
./deploy-stack.sh --vpc-stack my-mwaa --setup-azure-sso

# 3. Upload files to S3
./deploy-stack.sh --vpc-stack my-mwaa --upload

# 4. Build and deploy Lambda layer (requires Docker)
./deploy-stack.sh --vpc-stack my-mwaa --lambda-layer

# 5. Deploy ALB and authentication
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --alb

# 6. Update Lambda code (if needed)
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --update-lambda-code
```

### 3. Post-Deployment: Enable DAGs

**IMPORTANT**: After deployment completes, you must manually enable the DAGs from the Airflow UI:

1. Access the MWAA Airflow UI via the ALB URL (from CloudFormation outputs):
   ```
   https://<alb-dns>/aws_mwaa/aws-console-sso
   ```

2. In the Airflow UI, enable the following DAGs by clicking the toggle switch next to each:
   - `create_glue_connection` - **Enable this first and let it run once automatically**
   - `create_role_glue_job` - Enable for manual triggering when creating custom roles
   - `hello_world_simple` - Sample DAG
   - `hello_world_advanced` - Sample DAG

3. The `create_glue_connection` DAG will run automatically once enabled (it has `schedule='@once'`). This creates the Glue connection needed for database access.

**Why manual enabling is needed**: The MWAA environment configuration `core.dags_are_paused_at_creation=False` only affects NEW DAGs uploaded AFTER the configuration is applied. DAGs that existed before the configuration change remain paused and must be manually enabled once.

### 4. Create Custom Roles

**Prerequisites**: The `create_glue_connection` DAG must have run successfully (see step 3 above).

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

This creates:
- Custom role with specific DAG permissions
- Role permissions based on the source role (User) with restrictions to specific DAGs

### 5. Access MWAA

Navigate to the ALB URL (from CloudFormation outputs):
```
https://<alb-dns>/aws_mwaa/aws-console-sso
```

## Lambda Authorizer Versions

### V2 (Current - Direct Database Access)

**How It Works:**
1. User authenticates via Azure AD
2. Lambda extracts username from `custom:idp-name` JWT claim (Azure AD username)
3. Lambda retrieves database credentials from Glue connection `{env_name}_metadata_conn`
4. Lambda directly connects to MWAA metadata database via psycopg2
5. Lambda creates custom role with specific DAG permissions (if specified in GROUP_TO_ROLE_MAP)
6. Lambda assigns user to role in database
7. User is redirected to Airflow UI

**Benefits:**
- **20x faster**: 3-5 seconds vs 60-90 seconds
- **More reliable**: No DAG scheduler dependency
- **Simpler**: Single Lambda function
- **Auto-configuration**: Reads credentials from Glue connection
- **Granular Access**: Supports specific_dags for per-DAG permissions
- **Username Handling**: Extracts from custom:idp-name, truncates to 64 chars if needed

### V1 (DAG-Based - Backup)

**How It Works:**
1. User authenticates via Azure AD
2. Lambda triggers `update_user_role` DAG
3. DAG updates user role in database
4. Lambda waits for DAG completion
5. User is redirected to Airflow UI

**When to Use:**
- Complex role assignment logic
- Need audit trail via DAG runs
- Prefer Airflow-native approach

**Rollback to V1:**
```bash
cd airflow2/lambda_auth  # or airflow3/lambda_auth
mv lambda_mwaa-authorizer.py lambda_mwaa-authorizer_v2_backup.py
mv lambda_mwaa-authorizer_with_dag_original.py lambda_mwaa-authorizer.py

cd ../..
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --update-lambda-code
```

## Airflow Version Compatibility

### Airflow 2.x (airflow2/)
- Tested with Airflow 2.5.x through 2.10.x
- Uses standard authentication endpoints
- Compatible with `schedule_interval` and `DummyOperator`
- Direct database access via psycopg2 (V2)

### Airflow 3.x (airflow3/)
- Tested with Airflow 3.0.x
- **Updated authentication**: New `/auth/login/` endpoint
- **Updated DAG syntax**: Uses `schedule` instead of `schedule_interval`
- **Updated operators**: Uses `EmptyOperator` instead of `DummyOperator`
- **Database access**: Direct PostgreSQL via psycopg2 (bypasses `airflow-db-not-allowed`)
- **REST API changes**: Requires `logical_date` field when triggering DAGs
- Direct database access via psycopg2 (V2)

## Key Differences: Airflow 2.x vs 3.x

| Feature | Airflow 2.x | Airflow 3.x |
|---------|-------------|-------------|
| **Authentication Endpoints** | `/login/` | `/auth/login/` |
| **Plugin Paths** | `/aws_mwaa/aws-console-sso` | `/pluginsv2/aws_mwaa/aws-console-sso` |
| **DB Connection Env Var** | `AIRFLOW__CORE__SQL_ALCHEMY_CONN` | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` |
| **MWAA DB Access** | Direct SQL Alchemy | Blocked, use `DB_SECRETS` |
| **Database Access Method** | Direct PostgreSQL (V2) | Direct PostgreSQL (V2) |
| **REST API - Trigger DAG** | No `logical_date` required | Requires `logical_date` field |
| **Schedule Parameter** | `schedule_interval` | `schedule` (required) |
| **Dummy Operator** | `DummyOperator` | `EmptyOperator` (required) |
| **PythonOperator Context** | `provide_context=True` | Always provided (parameter removed) |

## Lambda Layer (psycopg2)

The solution uses a Lambda layer for the psycopg2 PostgreSQL driver to enable direct database access.

### Why a Lambda Layer?

psycopg2 requires compiled C extensions that must match the Lambda runtime environment (Amazon Linux 2). Building it in a Docker container ensures compatibility and resolves the common `No module named 'psycopg2._psycopg'` error.

### Dockerfile Configuration

The Dockerfile includes system dependencies for proper compilation:

```dockerfile
FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.11

# Install system dependencies for psycopg2
RUN yum install -y postgresql-devel gcc python3-devel

# Install psycopg2-binary for x86_64
RUN pip install --upgrade pip && \
    pip install psycopg2-binary==2.9.9 -t /python --no-cache-dir

# Create the layer structure
RUN mkdir -p /layer/python && cp -r /python/* /layer/python/
```

### Automatic Build Process

The `deploy-stack.sh` script automatically:
1. Checks for Docker availability
2. Builds Docker image with Lambda runtime and system dependencies
3. Installs psycopg2-binary with proper C extension compilation
4. Extracts layer and creates zip file
5. Uploads to S3 bucket
6. Publishes Lambda layer with version tracking
7. Attaches layer to Lambda function

**Command:**
```bash
./deploy-stack.sh --vpc-stack my-mwaa --lambda-layer
```

### Manual Build (if needed)

If you need to rebuild the layer manually:

```bash
cd airflow2/lambda_auth  # or airflow3/lambda_auth

# Build layer using Docker
docker build --platform linux/amd64 -t psycopg2-layer .
docker create --name psycopg2-container psycopg2-layer
docker cp psycopg2-container:/layer/. ./lambda-layer/
docker rm psycopg2-container
docker rmi psycopg2-layer

# Create zip
cd lambda-layer && zip -r ../psycopg2-layer.zip . -q && cd ..

# Upload to S3
aws s3 cp psycopg2-layer.zip s3://YOUR-BUCKET/lambda-layers/

# Publish layer
aws lambda publish-layer-version \
    --layer-name mwaa-psycopg2 \
    --description 'PostgreSQL adapter for MWAA authorizer' \
    --content S3Bucket=YOUR-BUCKET,S3Key=lambda-layers/psycopg2-layer.zip \
    --compatible-runtimes python3.11 python3.12

# Attach to Lambda
aws lambda update-function-configuration \
    --function-name YOUR-LAMBDA-FUNCTION \
    --layers arn:aws:lambda:REGION:ACCOUNT:layer:mwaa-psycopg2:VERSION
```

### Layer Structure

```
psycopg2-layer.zip
└── python/
    └── psycopg2/
        ├── __init__.py
        ├── _psycopg.cpython-311-x86_64-linux-gnu.so  # C extension
        └── ... (other files)
```

## Database Manager Helper Class

The `mwaa_database_manager.py` provides a clean, maintainable interface for database operations:

### Features

- **Context Manager**: Automatic connection and transaction handling
- **User Management**: Create or update users with automatic name population
- **Role Assignment**: Clean role assignment with automatic cleanup
- **Error Handling**: Comprehensive error handling and logging
- **Transaction Safety**: Automatic commit on success, rollback on error

### Usage Example

```python
from mwaa_database_manager import MWAADatabaseManager

# Context manager handles connection lifecycle
with MWAADatabaseManager(host, port, db, user, password) as db:
    # Update user role (creates user if doesn't exist)
    success = db.update_user_role(
        username='john.doe',
        role_name='Admin',
        email='john.doe@example.com'
    )
    # first_name and last_name automatically set to 'john.doe'
```

### Methods

**`get_or_create_user(username, email=None, first_name='', last_name='')`**
- Creates new user or returns existing user ID
- Updates email, first_name, last_name if provided
- Defaults first_name and last_name to username if not provided

**`get_role_id(role_name)`**
- Retrieves role ID by name
- Returns None if role doesn't exist

**`remove_all_user_roles(user_id)`**
- Removes all role assignments from user
- Ensures clean state before new assignment

**`assign_role_to_user(user_id, role_id)`**
- Assigns specific role to user
- Creates new role assignment record

**`update_user_role(username, role_name, email=None, first_name=None, last_name=None)`**
- Main method combining all operations
- Removes all existing roles
- Assigns only the specified role
- Defaults first_name and last_name to username

### Automatic Name Population

The database manager automatically populates user names:

```python
# If first_name and last_name not provided
db.update_user_role(username='john.doe', role_name='Admin')
# Result: first_name='john.doe', last_name='john.doe'

# If explicitly provided
db.update_user_role(
    username='john.doe', 
    role_name='Admin',
    first_name='John',
    last_name='Doe'
)
# Result: first_name='John', last_name='Doe'
```

### Transaction Management

The context manager ensures proper transaction handling:

```python
with MWAADatabaseManager(...) as db:
    db.update_user_role(...)  # Operations in transaction
    # Automatic commit on success
# Automatic rollback on exception
# Automatic connection close
```

### Error Handling

All methods include comprehensive error handling:

```python
try:
    with MWAADatabaseManager(...) as db:
        success = db.update_user_role(...)
        if not success:
            logger.error('Failed to update user role')
except Exception as e:
    logger.error(f'Database error: {e}')
    # Connection automatically closed
    # Transaction automatically rolled back
```

## VPC Configuration

### Network Requirements

The Lambda function requires specific VPC configuration for proper operation:

**Security Groups:**
- **MWAA Security Group**: Required for database access to MWAA metadata database
- **ALB Security Group**: Required for ALB communication

**Subnet Requirements:**
- Must be in a subnet with NAT Gateway for internet access
- Needs to reach ALB public keys endpoint: `public-keys.auth.elb.us-east-1.amazonaws.com`
- Must have route to MWAA VPC endpoint for database access

### Subnet Configuration

The VPC template creates three private subnets with different routing:

- **Subnet 1 & 2**: Private subnets without NAT Gateway
  - Used by MWAA workers
  - Route table: Local VPC + S3 VPC Endpoint only
  - No internet access

- **Subnet 3**: Private subnet with NAT Gateway
  - Used by Lambda function
  - Route table: Local VPC + S3 VPC Endpoint + NAT Gateway (0.0.0.0/0)
  - Internet access for ALB JWT verification

### CloudFormation Configuration

The ALB template configures Lambda VPC settings:

```yaml
VpcConfig:
  SecurityGroupIds: 
    - !ImportValue '${MWAAVPCStackName}-mwaa-sg'      # Database access
    - !ImportValue '${MWAAVPCStackName}-alb-sg'       # ALB communication
  SubnetIds: 
    - !ImportValue '${MWAAVPCStackName}-private-subnet-3'  # NAT Gateway access
```

### Why This Configuration?

The Lambda needs to access two different endpoints:

1. **MWAA Metadata Database** (via VPC endpoint)
   - Requires MWAA security group
   - Private communication within VPC
   - No internet required

2. **ALB Public Keys** (via internet)
   - Requires NAT Gateway for outbound internet
   - Validates JWT tokens from ALB
   - Must reach `public-keys.auth.elb.us-east-1.amazonaws.com`

### Common VPC Issues

**Issue:** `connection to server at "..." failed: timeout expired`
- **Cause**: Lambda missing MWAA security group
- **Solution**: Verify Lambda has both MWAA and ALB security groups

**Issue:** `Connection to public-keys.auth.elb.us-east-1.amazonaws.com timed out`
- **Cause**: Lambda not in subnet with NAT Gateway
- **Solution**: Ensure Lambda is in subnet-3 with NAT Gateway route

**Verification Commands:**
```bash
# Check Lambda VPC configuration
aws lambda get-function-configuration \
    --function-name YOUR-LAMBDA-NAME \
    --query 'VpcConfig.{SecurityGroups: SecurityGroupIds, Subnets: SubnetIds}'

# Check subnet route table
aws ec2 describe-route-tables \
    --filters "Name=association.subnet-id,Values=YOUR-SUBNET-ID" \
    --query 'RouteTables[0].Routes'

# Should show: 0.0.0.0/0 → NAT Gateway
```

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
5. Preserves `menu access on DAGs` permission (required for UI visibility)

## Deployment Options

### Command-Line Flags

- `--vpc` - Deploy VPC stack only
- `--upload` - Upload files to S3 only
- `--lambda-layer` - Build and deploy Lambda layer only
- `--alb` - Deploy ALB stack only
- `--update-lambda` - Update Lambda environment variables only
- `--update-lambda-code` - Update Lambda function code only
- `--create-cert` - Create self-signed SSL certificate and import to ACM
- `--setup-azure-sso` - Setup Azure Enterprise Application for SAML SSO
- `--vpc-stack NAME` - Specify VPC stack name (default: mwaa-vpc)
- `--alb-stack NAME` - Specify ALB stack name (default: <vpc-stack>-alb)

**Note:** Certificate creation and Azure SSO setup are automatically included in full deployment. They skip if VPC stack already exists.

### Examples

```bash
# Deploy with custom stack names
./deploy-stack.sh --vpc-stack prod-mwaa --alb-stack prod-mwaa-alb

# Deploy with Azure SSO automation
./deploy-stack.sh --vpc-stack prod-mwaa --alb-stack prod-mwaa-alb --setup-azure-sso

# Deploy only VPC
./deploy-stack.sh --vpc-stack dev-mwaa --vpc

# Create SSL certificate only
./deploy-stack.sh --vpc-stack dev-mwaa --create-cert

# Setup Azure SSO for existing VPC stack
./deploy-stack.sh --vpc-stack dev-mwaa --setup-azure-sso

# Rebuild Lambda layer
./deploy-stack.sh --vpc-stack prod-mwaa --lambda-layer

# Update Lambda configuration after role changes
./deploy-stack.sh --vpc-stack prod-mwaa --alb-stack prod-mwaa-alb --update-lambda
```

## Azure SSO Integration

### Overview

The solution includes automated Azure AD (Entra ID) SAML SSO setup that integrates seamlessly with the deployment process. The `--setup-azure-sso` flag automates the entire Azure Enterprise Application creation and configuration.

### Prerequisites

1. **Azure AD Tenant** with administrative access
2. **Azure CLI** installed and authenticated
3. **Python 3.8+** with required packages (auto-installed by script)
4. **Azure AD Permissions**:
   - `Application.ReadWrite.All` - Create and manage applications
   - `User.Read.All` - Read user information
   - `AppRoleAssignment.ReadWrite.All` - Assign users to applications

### Azure CLI Setup

#### Installation

**macOS:**
```bash
brew update && brew install azure-cli
```

**Linux (Ubuntu/Debian):**
```bash
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

**Windows:**
Download from: https://aka.ms/installazurecliwindows

#### Authentication

```bash
# Login to Azure
az login

# Verify authentication
az account show

# Set default subscription (if multiple)
az account set --subscription "Your Subscription Name"
```

### Automated Azure SSO Setup

The `--setup-azure-sso` flag automates the entire process:

```bash
# Full deployment with Azure SSO
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --setup-azure-sso
```

**What it does:**
1. Deploys VPC stack with MWAA and Cognito (or uses existing stack)
2. Retrieves Cognito User Pool ID and Domain from CloudFormation outputs
3. Retrieves ALB DNS from CloudFormation outputs
4. Creates Python virtual environment and installs dependencies automatically
5. Calls `azure_sso/create_nongallery_saml_app.py` with correct parameters
6. Creates Azure Enterprise Application with SAML configuration
7. **Generates 3-year valid SAML token signing certificate** automatically
8. Assigns users with names starting with "mwaa" to the application
9. Extracts Metadata URL, Login URL, and certificate details from Azure
10. Updates `deployment-config.json` with Azure SSO URLs automatically
11. Passes these URLs to ALB stack deployment
12. Configures Cognito SAML identity provider automatically

**Certificate Generation:**
The script automatically generates SAML token signing certificates using Azure's `addTokenSigningCertificate` API:
- Valid for 3 years
- Includes certificate thumbprint and expiration date
- Displays certificate in PEM format for manual configuration if needed
- No more certificate expiration errors!

**For existing VPC stack:**
```bash
# Setup Azure SSO only
./deploy-stack.sh --vpc-stack existing-vpc --setup-azure-sso

# Then deploy ALB with Azure SSO URLs
./deploy-stack.sh --vpc-stack existing-vpc --alb-stack my-alb --alb
```

### Manual Azure SSO Setup

If you prefer manual control or need to troubleshoot:

```bash
cd airflow3/azure_sso  # or airflow2/azure_sso

# Create virtual environment (if needed)
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the script
python create_nongallery_saml_app.py \
  --name "MWAA-Cognito-SAML" \
  --entity-id "urn:amazon:cognito:sp:us-east-1_XXXXX" \
  --reply-url "https://your-domain.auth.us-east-1.amazoncognito.com/saml2/idpresponse" \
  --sign-on-url "https://your-alb.elb.amazonaws.com" \
  --stack-name "my-mwaa"
```

**Parameters:**
- `--name`: Base name for the application (e.g., "MWAA-Cognito-SAML")
- `--entity-id`: SAML Entity ID from Cognito (format: `urn:amazon:cognito:sp:<user-pool-id>`)
- `--reply-url`: SAML Reply URL from Cognito (format: `https://<domain>.auth.<region>.amazoncognito.com/saml2/idpresponse`)
- `--sign-on-url`: ALB DNS URL (format: `https://<alb-dns>`)
- `--stack-name`: Stack identifier for uniqueness (optional, e.g., "prod", "dev", "af3-1")

### What the Azure SSO Script Does

1. **Checks for Existing Applications**
   - Searches for applications with the same Entity ID
   - Deletes existing applications to avoid conflicts
   - Ensures clean state for new deployment

2. **Creates App Registration**
   - Minimal backend configuration for SAML
   - No OAuth/OIDC properties (SAML only)

3. **Creates Enterprise Application**
   - Service Principal with SAML SSO mode
   - Visible in "Enterprise Applications" section

4. **Configures SAML Settings**
   - Entity ID (Identifier)
   - Reply URL (Assertion Consumer Service URL)
   - Sign-on URL
   - Security Group claims in SAML tokens

5. **Generates SAML Token Signing Certificate**
   - Creates 3-year valid certificate using `addTokenSigningCertificate` API
   - Retrieves certificate thumbprint and expiration date
   - Displays certificate in PEM format
   - Includes certificate details in JSON output

6. **Assigns Users**
   - Automatically assigns users with display names or userPrincipalName starting with "mwaa"
   - Uses default access role

7. **Generates URLs**
   - Federation Metadata URL for Cognito
   - Login URL for Azure AD
   - Entra Identifier
   - Logout URL
   - Certificate thumbprint and expiration

### Azure SSO Script Output

The script provides comprehensive output including:

```
✅ Non-Gallery SAML Application Created Successfully!

📋 Application Details:
Application Name:       MWAA-Cognito-SAML-my-mwaa
Tenant ID:              ae057b81-d76b-4aa7-911b-fce1a011a9a1
Application ID:         d7975fb3-ce1a-49aa-9e67-bd95db4e97f3
Client ID:              681eef66-2882-4f64-a840-e970b216b97e
Service Principal ID:   c644fbab-a046-4455-b46e-f82707fc5ccb

🔗 SAML Configuration:
Entity ID (Identifier): urn:amazon:cognito:sp:us-east-1_UibBymSE1
Reply URL (ACS URL):    https://af3-1-343218218212-domain.auth.us-east-1.amazoncognito.com/saml2/idpresponse
Sign-on URL:            https://af3-1-mwaa-alb-687043634.us-east-1.elb.amazonaws.com

📄 Federation Metadata URL (for AWS Cognito):
https://login.microsoftonline.com/ae057b81-d76b-4aa7-911b-fce1a011a9a1/federationmetadata/2007-06/federationmetadata.xml?appid=681eef66-2882-4f64-a840-e970b216b97e

🔐 Azure AD URLs (for Cognito SAML Identity Provider):
Login URL:                  https://login.microsoftonline.com/ae057b81-d76b-4aa7-911b-fce1a011a9a1/saml2
Microsoft Entra Identifier: https://sts.windows.net/ae057b81-d76b-4aa7-911b-fce1a011a9a1/
Logout URL:                 https://login.microsoftonline.com/ae057b81-d76b-4aa7-911b-fce1a011a9a1/saml2

🔐 SAML Token Signing Certificate:
Thumbprint:             A1B2C3D4E5F6G7H8I9J0K1L2M3N4O5P6Q7R8S9T0
Expiration:             2029-02-01T12:34:56Z
Status:                 Active
```

### Post-Setup Verification

#### 1. Verify in Azure Portal

Open the Enterprise Application link from script output and verify:

✅ **Single sign-on** page shows "**SAML-based Sign-on**"
✅ **Basic SAML Configuration** shows:
   - Identifier (Entity ID)
   - Reply URL (ACS URL)
   - Sign on URL

✅ **Attributes & Claims** includes:
   - Default claims (email, name, etc.)
   - Group membership claims (SecurityGroup)

✅ **Users and groups** shows assigned MWAA users

#### 2. Verify SAML Configuration via Script

```bash
cd airflow3/azure_sso
python verify_config.py <service-principal-id-from-output>
```

Example output:
```
Service Principal ID: c644fbab-a046-4455-b46e-f82707fc5ccb
Display Name: MWAA-Cognito-SAML-my-mwaa
SSO Mode: saml
Login URL: https://af3-1-mwaa-alb-687043634.us-east-1.elb.amazonaws.com
Reply URLs: ['https://af3-1-343218218212-domain.auth.us-east-1.amazoncognito.com/saml2/idpresponse']
```

#### 3. Test SSO Flow

1. Access your ALB URL: `https://<alb-dns>/aws_mwaa/aws-console-sso`
2. Should redirect to Azure AD login
3. After authentication, should redirect back to MWAA Airflow UI

### Multi-Stack Deployments

The `--stack-name` parameter creates unique applications for each environment:

```bash
# Production
./deploy-stack.sh --vpc-stack prod-mwaa --alb-stack prod-alb --setup-azure-sso

# Development
./deploy-stack.sh --vpc-stack dev-mwaa --alb-stack dev-alb --setup-azure-sso

# Testing
./deploy-stack.sh --vpc-stack test-mwaa --alb-stack test-alb --setup-azure-sso
```

This creates separate Azure applications:
- **MWAA-Cognito-SAML-prod-mwaa**
- **MWAA-Cognito-SAML-dev-mwaa**
- **MWAA-Cognito-SAML-test-mwaa**

### Azure SSO Troubleshooting

#### Authentication Errors

**Error:** "DefaultAzureCredential failed to retrieve a token"

**Solution:**
```bash
az logout
az login
az account show
```

#### Permission Errors

**Error:** "Insufficient privileges to complete the operation"

**Solution:** Grant required permissions:
```bash
# Via Azure Portal
1. Go to Azure AD → App registrations
2. Select your app
3. Click API permissions
4. Add required permissions
5. Click "Grant admin consent"

# Via Azure CLI
az ad app permission admin-consent --id <appId>
```

#### Application Already Exists

**Error:** "Another object with the same value for property identifierUris already exists"

**Solution:** The script automatically deletes existing apps. If it fails:
```bash
# List applications with the Entity ID
az ad app list --query "[?identifierUris[0]=='urn:amazon:cognito:sp:us-east-1_XXXXX'].{Name:displayName, ID:appId}"

# Delete manually
az ad app delete --id <appId>
```

#### Metadata URL Returns 404

**Cause:** Application not fully propagated or incorrect Client ID

**Solution:** Wait 5-10 minutes and try again. Verify Client ID in metadata URL matches script output.

### Azure SSO Security Considerations

1. **Credentials Management**
   - Use Azure CLI authentication for local development
   - Use Managed Identity for production automation
   - Never commit Azure credentials to version control

2. **Principle of Least Privilege**
   - Only grant minimum required permissions
   - Use `Application.ReadWrite.OwnedBy` if possible

3. **Multi-Factor Authentication**
   - Enable MFA for all Azure AD users accessing MWAA
   - Configure Conditional Access policies

4. **Audit Logging**
   - All actions logged in Azure AD Audit Logs
   - Review logs regularly for security monitoring

### Azure SSO Documentation

For detailed Azure SSO documentation, see:
- `airflow2/azure_sso/README.md` - Comprehensive Azure SSO guide (Airflow 2.x)
- `airflow3/azure_sso/README.md` - Comprehensive Azure SSO guide (Airflow 3.x)

Both folders contain the same Azure SSO automation scripts and documentation.

## References

### Command-Line Flags

- `--vpc` - Deploy VPC stack only
- `--upload` - Upload files to S3 only
- `--lambda-layer` - Build and deploy Lambda layer only
- `--alb` - Deploy ALB stack only
- `--update-lambda` - Update Lambda environment variables only
- `--update-lambda-code` - Update Lambda function code only
- `--vpc-stack NAME` - Specify VPC stack name (default: mwaa-vpc)
- `--alb-stack NAME` - Specify ALB stack name (default: <vpc-stack>-alb)

### Examples

```bash
# Deploy with custom stack names
./deploy-stack.sh --vpc-stack prod-mwaa --alb-stack prod-mwaa-alb

# Deploy only VPC
./deploy-stack.sh --vpc-stack dev-mwaa --vpc

# Rebuild Lambda layer
./deploy-stack.sh --vpc-stack prod-mwaa --lambda-layer

# Update Lambda configuration after role changes
./deploy-stack.sh --vpc-stack prod-mwaa --alb-stack prod-mwaa-alb --update-lambda
```

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

### Database Connection Issues

**Error:** `connection to server at "..." failed: timeout expired`

**Solution:**
1. Verify Lambda has MWAA security group
2. Check Lambda is in subnet-3 (with NAT Gateway)
3. Verify Glue connection exists and has correct network configuration

```bash
# Check Lambda VPC configuration
aws lambda get-function-configuration \
    --function-name YOUR-LAMBDA-NAME \
    --query 'VpcConfig.{SecurityGroups: SecurityGroupIds, Subnets: SubnetIds}'

# Verify Glue connection
aws glue get-connection --name {ENV}_metadata_conn
```

### psycopg2 Module Not Found

**Error:** `No module named 'psycopg2._psycopg'`

**Solution:** Rebuild and deploy Lambda layer:
```bash
./deploy-stack.sh --vpc-stack my-mwaa --lambda-layer
```

**Verify layer is attached:**
```bash
aws lambda get-function-configuration \
    --function-name YOUR-LAMBDA-NAME \
    --query 'Layers[*].Arn'
```

### ALB Public Keys Timeout

**Error:** `Connection to public-keys.auth.elb.us-east-1.amazonaws.com timed out`

**Solution:**
- Ensure Lambda is in subnet-3 (has NAT Gateway)
- Verify route table has 0.0.0.0/0 → NAT Gateway

```bash
# Check subnet route table
aws ec2 describe-route-tables \
    --filters "Name=association.subnet-id,Values=YOUR-SUBNET-ID" \
    --query 'RouteTables[0].Routes'
```

### Glue Connection Not Found

**Error:** `Glue connection not found`

**Solution:**
1. Run `create_role_glue_job` DAG once
2. Verify connection exists:
```bash
aws glue get-connection --name {ENV}_metadata_conn
```

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
2. Ensure you have the latest `create_role_glue_job_dag.py`
3. Re-run the `create_role_glue_job` DAG
4. Verify role has `menu access on DAGs` permission

### Users Have Multiple Roles

**Error:** User has both Public and custom roles

**Root Cause:** `update_user_role` not removing all existing roles

**Solution:**
1. Ensure you have the latest `update_user_role_dag.py` or use V2 Lambda
2. Manually remove extra roles from user (Security → List Users → Edit)
3. Or trigger role assignment again

## Migration from Airflow 2.x to 3.x

### Checklist

- [ ] Update MWAA environment to Airflow 3.x
- [ ] Deploy updated ALB configuration (handles new endpoints)
- [ ] Replace `schedule_interval` with `schedule` in all DAGs
- [ ] Remove `provide_context=True` from all PythonOperator instances
- [ ] Replace `DummyOperator` with `EmptyOperator`
- [ ] Update Lambda authorizer (already compatible)
- [ ] Test authentication flow
- [ ] Verify role creation DAG works
- [ ] Test custom role users can see DAGs page

### Deployment

```bash
cd airflow3
./deploy-stack.sh --vpc-stack mwaa-af3 --alb-stack mwaa-af3-alb
```

## Security Considerations

1. **Secrets Management**: Deployment config contains sensitive data - keep it secure
2. **IAM Permissions**: Follow principle of least privilege
3. **Network Security**: MWAA uses private subnets with VPC endpoints
4. **ALB Security**: HTTPS only with valid ACM certificate
5. **Database Access**: Credentials from Glue connection (encrypted)
6. **Lambda Layer**: Built with platform-specific binaries for security

## Cost Optimization

- Use appropriate MWAA environment size
- Configure auto-scaling for workers
- Use S3 lifecycle policies for logs
- Delete unused Glue connections and jobs
- Monitor CloudWatch logs retention
- Use Lambda layers to reduce deployment package size

## Performance Comparison

| Metric | V1 (DAG-based) | V2 (Direct DB) |
|--------|----------------|----------------|
| Login Time | 60-90 seconds | 3-5 seconds |
| Database Queries | Via DAG | Direct |
| Failure Points | 3 (Lambda, DAG, Glue) | 1 (Lambda) |
| Dependencies | DAG scheduler | Database only |
| Complexity | High | Low |
| Maintenance | Multiple files | Centralized |

## Contributing

When contributing:
1. Test changes in both Airflow 2.x and 3.x
2. Update both airflow2/ and airflow3/ folders
3. Update main README
4. Follow existing code style
5. Test deployment and cleanup scripts
6. Ensure Lambda layer builds correctly

## References

- [AWS MWAA Documentation](https://docs.aws.amazon.com/mwaa/)
- [Integrate Amazon MWAA with Microsoft Entra ID using SAML authentication](https://aws.amazon.com/blogs/big-data/integrate-amazon-mwaa-with-microsoft-entra-id-using-saml-authentication/)
- [Limit access to Apache Airflow DAGs in Amazon MWAA](https://docs.aws.amazon.com/mwaa/latest/userguide/limit-access-to-dags.html)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Azure AD SAML Configuration](https://docs.microsoft.com/en-us/azure/active-directory/saas-apps/)

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review CloudFormation stack events
3. Check CloudWatch logs for Lambda and Glue jobs
4. Verify Azure AD configuration
5. Ensure Docker is installed for Lambda layer builds

## License

This solution is provided as-is for use with AWS services.
