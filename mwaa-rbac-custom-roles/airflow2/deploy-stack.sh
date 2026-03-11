#!/bin/bash

# AWS MWAA Custom RBAC Solution Deployment Script
# This script deploys the complete MWAA RBAC solution using CloudFormation templates
#
# Usage:
#   ./deploy-stack.sh                                    # Run all steps sequentially
#   ./deploy-stack.sh --vpc-stack my-vpc --alb-stack my-alb  # Custom stack names
#   ./deploy-stack.sh --vpc                              # Deploy VPC stack only
#   ./deploy-stack.sh --upload                           # Upload files to S3 only
#   ./deploy-stack.sh --alb                              # Deploy ALB stack only
#   ./deploy-stack.sh --vpc --upload                     # Deploy VPC and upload files
#   ./deploy-stack.sh --help                             # Show usage information

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Deployment flags
RUN_VPC=false
RUN_UPLOAD=false
RUN_LAMBDA_LAYER=false
RUN_ALB=false
RUN_LAMBDA_UPDATE=false
RUN_LAMBDA_CODE_UPDATE=false
RUN_AZURE_SSO=false
RUN_CREATE_CERT=false
RUN_ALL=true

# Stack names (defaults)
VPC_STACK_NAME=""
ALB_STACK_NAME=""

# Azure SSO URLs (populated by setup_azure_sso function)
AZURE_METADATA_URL=""
AZURE_LOGIN_URL=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --vpc)
            RUN_VPC=true
            RUN_ALL=false
            shift
            ;;
        --upload)
            RUN_UPLOAD=true
            RUN_ALL=false
            shift
            ;;
        --lambda-layer)
            RUN_LAMBDA_LAYER=true
            RUN_ALL=false
            shift
            ;;
        --alb)
            RUN_ALB=true
            RUN_ALL=false
            shift
            ;;
        --update-lambda)
            RUN_LAMBDA_UPDATE=true
            RUN_ALL=false
            shift
            ;;
        --update-lambda-code)
            RUN_LAMBDA_CODE_UPDATE=true
            RUN_ALL=false
            shift
            ;;
        --setup-azure-sso)
            RUN_AZURE_SSO=true
            shift
            ;;
        --create-cert)
            RUN_CREATE_CERT=true
            shift
            ;;
        --vpc-stack)
            VPC_STACK_NAME="$2"
            shift 2
            ;;
        --alb-stack)
            ALB_STACK_NAME="$2"
            shift 2
            ;;
        --help|-h)
            echo "AWS MWAA Custom RBAC Solution Deployment Script"
            echo ""
            echo "Usage:"
            echo "  ./deploy-stack.sh                                    # Run all steps sequentially"
            echo "  ./deploy-stack.sh --vpc-stack my-vpc --alb-stack my-alb  # Custom stack names"
            echo "  ./deploy-stack.sh --vpc                              # Deploy VPC stack only"
            echo "  ./deploy-stack.sh --upload                           # Upload files to S3 only"
            echo "  ./deploy-stack.sh --lambda-layer                     # Build and deploy Lambda layer only"
            echo "  ./deploy-stack.sh --alb                              # Deploy ALB stack only"
            echo "  ./deploy-stack.sh --update-lambda                    # Update Lambda environment variables only"
            echo "  ./deploy-stack.sh --update-lambda-code               # Update Lambda function code only"
            echo "  ./deploy-stack.sh --vpc --upload                     # Deploy VPC and upload files"
            echo "  ./deploy-stack.sh --setup-azure-sso                  # Setup Azure SSO (run after VPC deployment)"
            echo "  ./deploy-stack.sh --create-cert                      # Create and import SSL certificate to ACM"
            echo "  ./deploy-stack.sh --help                             # Show this help message"
            echo ""
            echo "Options:"
            echo "  --vpc-stack NAME    VPC stack name (default: mwaa-vpc)"
            echo "  --alb-stack NAME    ALB stack name (default: <vpc-stack>-alb)"
            echo "  --setup-azure-sso   Setup Azure Enterprise Application for SAML SSO"
            echo "  --create-cert       Create self-signed certificate and import to ACM"
            echo ""
            echo "Steps:"
            echo "  --vpc      Deploy VPC and MWAA infrastructure stack"
            echo "  --upload   Upload Lambda code and DAG files to S3"
            echo "  --lambda-layer  Build and deploy Lambda layer with psycopg2"
            echo "  --alb      Deploy ALB and authentication stack"
            echo "  --update-lambda  Update Lambda environment variables only"
            echo "  --update-lambda-code  Update Lambda function code only"
            echo ""
            echo "Examples:"
            echo "  # Full deployment with default stack names"
            echo "  ./deploy-stack.sh"
            echo ""
            echo "  # Full deployment with custom stack names"
            echo "  ./deploy-stack.sh --vpc-stack my-mwaa-vpc --alb-stack my-mwaa-alb"
            echo ""
            echo "  # Full deployment with Azure SSO setup"
            echo "  ./deploy-stack.sh --vpc-stack my-vpc --alb-stack my-alb --setup-azure-sso"
            echo ""
            echo "  # Create SSL certificate and deploy"
            echo "  ./deploy-stack.sh --vpc-stack my-vpc --alb-stack my-alb --create-cert"
            echo ""
            echo "  # Deploy VPC with custom name, then upload files, then deploy ALB"
            echo "  ./deploy-stack.sh --vpc-stack my-vpc --vpc"
            echo "  ./deploy-stack.sh --vpc-stack my-vpc --upload"
            echo "  ./deploy-stack.sh --vpc-stack my-vpc --alb-stack my-alb --alb"
            echo ""
            echo "  # Setup Azure SSO after VPC deployment"
            echo "  ./deploy-stack.sh --vpc-stack my-vpc --setup-azure-sso"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# If no flags provided, run all steps
if [[ "$RUN_ALL" == true ]]; then
    RUN_VPC=true
    RUN_UPLOAD=true
    RUN_LAMBDA_LAYER=true
    RUN_ALB=true
fi

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if required files exist
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    local missing_files=()
    
    if [[ ! -f "deployment-config.json" ]]; then
        missing_files+=("deployment-config.json")
    fi
    
    if [[ ! -f "01-vpc-mwaa.yml" ]]; then
        missing_files+=("01-vpc-mwaa.yml")
    fi
    
    if [[ ! -f "02-alb.yml" ]]; then
        missing_files+=("02-alb.yml")
    fi
    
    if [[ ! -f "lambda_auth/lambda_mwaa-authorizer.py" ]]; then
        missing_files+=("lambda_auth/lambda_mwaa-authorizer.py")
    fi
    
    if [[ ! -f "lambda_auth/mwaa_database_manager.py" ]]; then
        missing_files+=("lambda_auth/mwaa_database_manager.py")
    fi
    
    if [[ ! -f "lambda_auth/Dockerfile" ]]; then
        missing_files+=("lambda_auth/Dockerfile")
    fi
    
    if [[ ! -f "role_creation_dag/create_role_glue_job_dag.py" ]]; then
        missing_files+=("role_creation_dag/create_role_glue_job_dag.py")
    fi
    
    if [[ ${#missing_files[@]} -gt 0 ]]; then
        print_error "Missing required files:"
        for file in "${missing_files[@]}"; do
            echo "  - $file"
        done
        exit 1
    fi
    
    # Check if AWS CLI is installed
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    
    # Check if jq is installed
    if ! command -v jq &> /dev/null; then
        print_error "jq is not installed. Please install it first."
        exit 1
    fi
    
    print_success "All prerequisites met"
}

# Function to create and import SSL certificate to ACM
create_and_import_certificate() {
    print_status "Creating self-signed SSL certificate for ALB..."
    
    # Check if openssl is available
    if ! command -v openssl &> /dev/null; then
        print_error "OpenSSL is not installed. Please install it first."
        return 1
    fi
    
    local cert_dir="ssl-certs"
    mkdir -p "$cert_dir"
    
    local private_key="$cert_dir/private-key.pem"
    local certificate="$cert_dir/certificate.pem"
    
    # Generate self-signed certificate
    print_status "Generating self-signed certificate (valid for 365 days)..."
    openssl req -x509 -newkey rsa:2048 \
        -keyout "$private_key" \
        -out "$certificate" \
        -days 365 \
        -nodes \
        -subj "/C=US/ST=Virginia/L=Arlington/O=MWAA/CN=*.elb.amazonaws.com" \
        2>/dev/null
    
    if [[ $? -ne 0 ]]; then
        print_error "Failed to generate certificate"
        return 1
    fi
    
    print_success "Certificate generated successfully"
    
    # Import certificate to ACM
    print_status "Importing certificate to AWS Certificate Manager..."
    local cert_arn=$(aws acm import-certificate \
        --certificate fileb://"$certificate" \
        --private-key fileb://"$private_key" \
        --region "$AWS_REGION" \
        --query 'CertificateArn' \
        --output text 2>&1)
    
    if [[ $? -ne 0 ]]; then
        print_error "Failed to import certificate to ACM"
        echo "$cert_arn"
        return 1
    fi
    
    print_success "Certificate imported to ACM"
    print_status "Certificate ARN: $cert_arn"
    
    # Update deployment-config.json with certificate ARN
    print_status "Updating deployment-config.json with certificate ARN..."
    local temp_config=$(mktemp)
    jq --arg cert_arn "$cert_arn" '
        map(if .ParameterKey == "ALBCertificateArn" then .ParameterValue = $cert_arn else . end)
    ' deployment-config.json > "$temp_config"
    mv "$temp_config" deployment-config.json
    
    print_success "Updated deployment-config.json with certificate ARN"
    
    # Store certificate ARN for later use
    echo "$cert_arn" > "$cert_dir/certificate-arn.txt"
    
    print_status "Certificate files saved in: $cert_dir/"
    print_warning "Note: This is a self-signed certificate. For production, use a certificate from a trusted CA."
    
    return 0
}

# Function to parse deployment configuration
parse_config() {
    print_status "Parsing deployment configuration..."
    
    if [[ ! -f "deployment-config.json" ]]; then
        print_error "deployment-config.json not found"
        exit 1
    fi
    
    # Set default stack names if not provided
    if [[ -z "$VPC_STACK_NAME" ]]; then
        VPC_STACK_NAME="mwaa-vpc"
    fi
    
    if [[ -z "$ALB_STACK_NAME" ]]; then
        ALB_STACK_NAME="${VPC_STACK_NAME}-alb"
    fi
    
    # Use VPC stack name as MWAA environment name
    MWAA_ENV_NAME="$VPC_STACK_NAME"
    
    # Update MWAAEnvironmentName in deployment-config.json to match VPC stack name
    print_status "Updating MWAAEnvironmentName in deployment-config.json to: $MWAA_ENV_NAME"
    local temp_config=$(mktemp)
    jq --arg env_name "$MWAA_ENV_NAME" '
        map(if .ParameterKey == "MWAAEnvironmentName" then .ParameterValue = $env_name else . end)
    ' deployment-config.json > "$temp_config"
    mv "$temp_config" deployment-config.json
    
    AWS_REGION=$(aws configure get region || echo "us-east-1")
    
    print_status "VPC Stack Name: $VPC_STACK_NAME"
    print_status "ALB Stack Name: $ALB_STACK_NAME"
    print_status "MWAA Environment: $MWAA_ENV_NAME"
    print_status "AWS Region: $AWS_REGION"
}

# Function to wait for stack completion
wait_for_stack() {
    local stack_name=$1
    local operation=$2
    
    print_status "Waiting for stack $stack_name to complete $operation..."
    
    local status=""
    local count=0
    local max_attempts=120  # 60 minutes max (30 second intervals)
    
    while [[ $count -lt $max_attempts ]]; do
        status=$(aws cloudformation describe-stacks --stack-name "$stack_name" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "STACK_NOT_FOUND")
        
        case $status in
            *_COMPLETE)
                if [[ $status == *"ROLLBACK"* ]]; then
                    print_error "Stack $stack_name failed with status: $status"
                    return 1
                else
                    print_success "Stack $stack_name completed with status: $status"
                    return 0
                fi
                ;;
            *_IN_PROGRESS)
                echo -n "."
                ;;
            *_FAILED)
                print_error "Stack $stack_name failed with status: $status"
                return 1
                ;;
            "STACK_NOT_FOUND")
                if [[ $operation == "delete" ]]; then
                    print_success "Stack $stack_name has been deleted"
                    return 0
                else
                    print_error "Stack $stack_name not found"
                    return 1
                fi
                ;;
            *)
                print_warning "Unknown status: $status"
                ;;
        esac
        
        sleep 30
        ((count++))
    done
    
    print_error "Timeout waiting for stack $stack_name to complete"
    return 1
}

# Function to get S3 bucket name from VPC stack outputs
get_s3_bucket_name() {
    local bucket_name=$(aws cloudformation describe-stacks \
        --stack-name "$VPC_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`MwaaS3BucketName`].OutputValue' \
        --output text 2>/dev/null)
    
    if [[ -z "$bucket_name" || "$bucket_name" == "None" ]]; then
        print_error "Could not retrieve S3 bucket name from VPC stack outputs"
        exit 1
    fi
    
    echo "$bucket_name"
}

# Function to upload files to S3
upload_files_to_s3() {
    local bucket_name=$1
    
    print_status "Uploading Lambda function and DAG files to S3..."
    
    # Upload Lambda function code
    print_status "Uploading lambda_mwaa-authorizer.py..."
    aws s3 cp lambda_auth/lambda_mwaa-authorizer.py "s3://$bucket_name/lambda-code/lambda_mwaa-authorizer.py" \
        --content-type "text/x-python"
    
    print_status "Uploading mwaa_database_manager.py..."
    aws s3 cp lambda_auth/mwaa_database_manager.py "s3://$bucket_name/lambda-code/mwaa_database_manager.py" \
        --content-type "text/x-python"
    
    # Upload DAG files
    print_status "Uploading DAG files..."
    
    # Upload create_glue_connection_dag.py
    print_status "  - create_glue_connection_dag.py..."
    aws s3 cp role_creation_dag/create_glue_connection_dag.py "s3://$bucket_name/dags/create_glue_connection_dag.py" \
        --content-type "text/x-python"
    
    # Upload create_role_glue_job_dag.py with stack name replacement
    print_status "  - create_role_glue_job_dag.py (replacing {{VPC_STACK_NAME}} with $VPC_STACK_NAME)..."
    sed "s/{{VPC_STACK_NAME}}/$VPC_STACK_NAME/g" role_creation_dag/create_role_glue_job_dag.py | \
        aws s3 cp - "s3://$bucket_name/dags/create_role_glue_job_dag.py" \
        --content-type "text/x-python"
    
    # Upload update_user_role_dag.py
    print_status "  - update_user_role_dag.py..."
    aws s3 cp role_creation_dag/update_user_role_dag.py "s3://$bucket_name/dags/update_user_role_dag.py" \
        --content-type "text/x-python"
    
    # Upload sample DAGs
    print_status "  - hello_world_simple.py..."
    aws s3 cp sample_dags/hello_world_simple.py "s3://$bucket_name/dags/hello_world_simple.py" \
        --content-type "text/x-python"
    
    print_status "  - hello_world_advanced.py..."
    aws s3 cp sample_dags/hello_world_advanced.py "s3://$bucket_name/dags/hello_world_advanced.py" \
        --content-type "text/x-python"
    
    print_success "Files uploaded successfully"
}

# Function to build and deploy Lambda layer
build_and_deploy_lambda_layer() {
    print_status "Building and deploying Lambda layer with psycopg2..."
    
    # Get S3 bucket name
    local bucket_name=$(get_s3_bucket_name)
    print_status "Using S3 bucket: $bucket_name"
    
    # Check if Docker is available
    if ! command -v docker &> /dev/null; then
        print_error "Docker is required to build the Lambda layer"
        print_error "Please install Docker: https://docs.docker.com/get-docker/"
        return 1
    fi
    
    # Check if Dockerfile exists
    if [[ ! -f "lambda_auth/Dockerfile" ]]; then
        print_error "Dockerfile not found at lambda_auth/Dockerfile"
        return 1
    fi
    
    local layer_dir="lambda_auth/lambda-layer"
    local zip_file="lambda_auth/psycopg2-layer.zip"
    
    # Clean up existing layer directory and zip
    if [[ -d "$layer_dir" ]]; then
        print_status "Cleaning up existing layer directory..."
        rm -rf "$layer_dir"
    fi
    
    if [[ -f "$zip_file" ]]; then
        print_status "Removing existing layer zip..."
        rm -f "$zip_file"
    fi
    
    # Build Docker image
    print_status "Building Docker image with Lambda runtime (this may take a few minutes)..."
    if ! docker build -t psycopg2-layer lambda_auth/ > /dev/null 2>&1; then
        print_error "Docker build failed"
        return 1
    fi
    
    print_success "Docker image built successfully"
    
    # Extract layer from Docker container
    print_status "Extracting layer from Docker container..."
    docker create --name psycopg2-container psycopg2-layer > /dev/null 2>&1
    docker cp psycopg2-container:/layer/. "$layer_dir/" > /dev/null 2>&1
    
    # Clean up Docker resources
    docker rm psycopg2-container > /dev/null 2>&1
    docker rmi psycopg2-layer > /dev/null 2>&1
    
    # Create zip file
    print_status "Creating layer zip file..."
    (cd "$layer_dir" && zip -r "../psycopg2-layer.zip" . -q)
    
    if [[ $? -ne 0 ]]; then
        print_error "Failed to create layer zip file"
        rm -rf "$layer_dir"
        return 1
    fi
    
    # Get zip file size
    local zip_size=$(du -h "$zip_file" | cut -f1)
    print_success "Lambda layer built successfully (Size: $zip_size)"
    
    # Clean up layer directory
    rm -rf "$layer_dir"
    
    # Upload to S3
    print_status "Uploading Lambda layer to S3..."
    if ! aws s3 cp "$zip_file" "s3://$bucket_name/lambda-layers/psycopg2-layer.zip"; then
        print_error "Failed to upload Lambda layer to S3"
        return 1
    fi
    
    print_success "Lambda layer uploaded to S3"
    
    # Publish Lambda layer
    print_status "Publishing Lambda layer..."
    local layer_output=$(aws lambda publish-layer-version \
        --layer-name mwaa-psycopg2 \
        --description 'PostgreSQL adapter for MWAA authorizer' \
        --content S3Bucket="$bucket_name",S3Key=lambda-layers/psycopg2-layer.zip \
        --compatible-runtimes python3.11 python3.12 \
        --query '{LayerVersionArn: LayerVersionArn, Version: Version}' \
        --output json 2>&1)
    
    if [[ $? -ne 0 ]]; then
        print_error "Failed to publish Lambda layer"
        echo "$layer_output"
        return 1
    fi
    
    local layer_arn=$(echo "$layer_output" | jq -r '.LayerVersionArn')
    local layer_version=$(echo "$layer_output" | jq -r '.Version')
    
    print_success "Lambda layer published successfully"
    print_status "Layer ARN: $layer_arn"
    print_status "Layer Version: $layer_version"
    
    # Get Lambda function name from ALB stack (if it exists)
    local lambda_name=$(aws cloudformation describe-stacks \
        --stack-name "$ALB_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionName`].OutputValue' \
        --output text 2>/dev/null)
    
    if [[ -n "$lambda_name" && "$lambda_name" != "None" ]]; then
        print_status "Attaching layer to Lambda function: $lambda_name"
        
        if aws lambda update-function-configuration \
            --function-name "$lambda_name" \
            --layers "$layer_arn" \
            --query '{FunctionName: FunctionName, Layers: Layers[*].Arn}' \
            --output json > /dev/null 2>&1; then
            
            print_success "Lambda layer attached to function successfully"
        else
            print_warning "Could not attach layer to Lambda function (it may not exist yet)"
            print_status "You can attach it later with:"
            echo "  aws lambda update-function-configuration \\"
            echo "    --function-name YOUR-LAMBDA-FUNCTION \\"
            echo "    --layers $layer_arn"
        fi
    else
        print_warning "Lambda function not found (ALB stack may not be deployed yet)"
        print_status "After deploying the ALB stack, attach the layer with:"
        echo "  aws lambda update-function-configuration \\"
        echo "    --function-name YOUR-LAMBDA-FUNCTION \\"
        echo "    --layers $layer_arn"
    fi
    
    print_success "Lambda layer deployment completed"
}

# Function to deploy VPC stack
deploy_vpc_stack() {
    print_status "Deploying VPC stack: $VPC_STACK_NAME"
    
    # Filter parameters for VPC template only
    local vpc_params=$(mktemp)
    jq '[.[] | select(.ParameterKey | IN("VpcCIDR", "PrivateSubnet1CIDR", "PrivateSubnet2CIDR", "PrivateSubnet3CIDR", "PublicSubnet1CIDR", "PublicSubnet2CIDR", "MWAAEnvironmentName"))]' deployment-config.json > "$vpc_params"
    
    # Check if stack already exists
    if aws cloudformation describe-stacks --stack-name "$VPC_STACK_NAME" &>/dev/null; then
        print_warning "Stack $VPC_STACK_NAME already exists. Updating..."
        aws cloudformation update-stack \
            --stack-name "$VPC_STACK_NAME" \
            --template-body file://01-vpc-mwaa.yml \
            --parameters file://"$vpc_params" \
            --capabilities CAPABILITY_NAMED_IAM
    else
        print_status "Creating new stack: $VPC_STACK_NAME"
        aws cloudformation create-stack \
            --stack-name "$VPC_STACK_NAME" \
            --template-body file://01-vpc-mwaa.yml \
            --parameters file://"$vpc_params" \
            --capabilities CAPABILITY_NAMED_IAM
    fi
    
    # Clean up temp file
    rm -f "$vpc_params"
    
    # Wait for stack completion
    if ! wait_for_stack "$VPC_STACK_NAME" "create/update"; then
        print_error "VPC stack deployment failed"
        exit 1
    fi
    
    print_success "VPC stack deployed successfully"
}

# Function to setup Azure SSO
setup_azure_sso() {
    print_status "Setting up Azure Enterprise Application for SAML SSO..."
    
    # Check if Python 3 is available
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is required for Azure SSO setup"
        return 1
    fi
    
    # Check if azure_sso directory exists
    if [[ ! -d "azure_sso" ]]; then
        print_error "azure_sso directory not found"
        return 1
    fi
    
    # Check if create_nongallery_saml_app.py exists
    if [[ ! -f "azure_sso/create_nongallery_saml_app.py" ]]; then
        print_error "azure_sso/create_nongallery_saml_app.py not found"
        return 1
    fi
    
    # Check if required Python packages are installed
    print_status "Checking Python dependencies..."
    
    # Always use virtual environment for Azure SSO
    if [[ ! -d "azure_sso/.venv" ]]; then
        print_status "Creating Python virtual environment..."
        python3 -m venv azure_sso/.venv
        
        if [[ $? -ne 0 ]]; then
            print_error "Failed to create virtual environment"
            return 1
        fi
    fi
    
    # Install/update dependencies in virtual environment
    print_status "Installing/updating dependencies in virtual environment..."
    azure_sso/.venv/bin/pip install -q --upgrade pip
    azure_sso/.venv/bin/pip install -q -r azure_sso/requirements.txt
    
    if [[ $? -ne 0 ]]; then
        print_error "Failed to install Python dependencies"
        return 1
    fi
    
    print_success "Python dependencies ready"
    
    # Get Cognito User Pool ID from VPC stack outputs
    print_status "Retrieving Cognito User Pool ID from VPC stack..."
    local user_pool_id=$(aws cloudformation describe-stacks \
        --stack-name "$VPC_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`UserPoolId`].OutputValue' \
        --output text 2>/dev/null)
    
    if [[ -z "$user_pool_id" || "$user_pool_id" == "None" ]]; then
        print_error "Could not retrieve Cognito User Pool ID from VPC stack"
        return 1
    fi
    
    print_status "Cognito User Pool ID: $user_pool_id"
    
    # Get Cognito Domain from VPC stack outputs
    print_status "Retrieving Cognito Domain from VPC stack..."
    local cognito_domain=$(aws cloudformation describe-stacks \
        --stack-name "$VPC_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`UserPoolDomain`].OutputValue' \
        --output text 2>/dev/null)
    
    if [[ -z "$cognito_domain" || "$cognito_domain" == "None" ]]; then
        print_error "Could not retrieve Cognito Domain from VPC stack"
        return 1
    fi
    
    print_status "Cognito Domain: $cognito_domain"
    
    # Get ALB DNS from VPC stack outputs
    print_status "Retrieving ALB DNS from VPC stack..."
    local alb_dns=$(aws cloudformation describe-stacks \
        --stack-name "$VPC_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`ApplicationLoadBalancerUrl`].OutputValue' \
        --output text 2>/dev/null)
    
    if [[ -z "$alb_dns" || "$alb_dns" == "None" ]]; then
        print_error "Could not retrieve ALB DNS from VPC stack"
        return 1
    fi
    
    print_status "ALB DNS: $alb_dns"
    
    # Construct URLs
    local entity_id="urn:amazon:cognito:sp:${user_pool_id}"
    local reply_url="https://${cognito_domain}.auth.${AWS_REGION}.amazoncognito.com/saml2/idpresponse"
    local sign_on_url="https://${alb_dns}"
    
    print_status "Calling Azure SSO setup script..."
    print_status "  Entity ID: $entity_id"
    print_status "  Reply URL: $reply_url"
    print_status "  Sign-on URL: $sign_on_url"
    print_status "  Stack Name: $VPC_STACK_NAME"
    echo
    
    # Always use virtual environment Python
    local python_cmd="azure_sso/.venv/bin/python3"
    
    # Capture the output
    local azure_output=$(mktemp)
    local azure_json=$(mktemp)
    
    if $python_cmd azure_sso/create_nongallery_saml_app.py \
        --name "MWAA-Cognito-SAML" \
        --entity-id "$entity_id" \
        --reply-url "$reply_url" \
        --sign-on-url "$sign_on_url" \
        --stack-name "$VPC_STACK_NAME" 2>&1 | tee "$azure_output"; then
        
        print_success "Azure SSO setup completed"
        
        # Extract JSON output from the script (between the JSON Output markers)
        # The JSON is printed between two separator lines after "JSON Output (for automation):"
        local json_start=$(grep -n "^JSON Output (for automation):$" "$azure_output" | tail -1 | cut -d: -f1)
        
        if [[ -n "$json_start" ]]; then
            # Find the start of JSON (after the separator line)
            local json_start_line=$((json_start + 2))
            
            # Extract everything from json_start_line to the last separator line
            # First, find the line number of the last separator
            local last_separator=$(grep -n "^=\+$" "$azure_output" | tail -1 | cut -d: -f1)
            
            if [[ -n "$last_separator" ]] && [[ $last_separator -gt $json_start_line ]]; then
                local json_end_line=$((last_separator - 1))
                
                # Extract JSON lines (they may be wrapped)
                sed -n "${json_start_line},${json_end_line}p" "$azure_output" > "$azure_json"
                
                # Try to parse as-is first (in case it's already valid JSON)
                if jq empty "$azure_json" 2>/dev/null; then
                    # Parse JSON to extract URLs
                    AZURE_METADATA_URL=$(jq -r '.metadata_url' "$azure_json")
                    AZURE_LOGIN_URL=$(jq -r '.login_url' "$azure_json")
                    local cert_thumbprint=$(jq -r '.certificate_thumbprint // empty' "$azure_json")
                    local cert_expiration=$(jq -r '.certificate_expiration // empty' "$azure_json")
                    
                    if [[ -n "$AZURE_METADATA_URL" ]] && [[ "$AZURE_METADATA_URL" != "null" ]]; then
                        print_success "Extracted Metadata URL: $AZURE_METADATA_URL"
                    else
                        print_warning "Could not extract Metadata URL from JSON output"
                    fi
                    
                    if [[ -n "$AZURE_LOGIN_URL" ]] && [[ "$AZURE_LOGIN_URL" != "null" ]]; then
                        print_success "Extracted Login URL: $AZURE_LOGIN_URL"
                    else
                        print_warning "Could not extract Login URL from JSON output"
                    fi
                    
                    if [[ -n "$cert_thumbprint" ]] && [[ "$cert_thumbprint" != "null" ]]; then
                        print_success "Certificate Thumbprint: $cert_thumbprint"
                    fi
                    
                    if [[ -n "$cert_expiration" ]] && [[ "$cert_expiration" != "null" ]]; then
                        print_success "Certificate Expiration: $cert_expiration"
                    fi
                else
                    # JSON is wrapped - try to unwrap it
                    cat "$azure_json" | tr -d '\n' | sed 's/  */ /g' > "${azure_json}.unwrapped"
                    echo "" >> "${azure_json}.unwrapped"
                    
                    if jq empty "${azure_json}.unwrapped" 2>/dev/null; then
                        # Parse unwrapped JSON
                        AZURE_METADATA_URL=$(jq -r '.metadata_url' "${azure_json}.unwrapped")
                        AZURE_LOGIN_URL=$(jq -r '.login_url' "${azure_json}.unwrapped")
                        local cert_thumbprint=$(jq -r '.certificate_thumbprint // empty' "${azure_json}.unwrapped")
                        local cert_expiration=$(jq -r '.certificate_expiration // empty' "${azure_json}.unwrapped")
                        
                        if [[ -n "$AZURE_METADATA_URL" ]] && [[ "$AZURE_METADATA_URL" != "null" ]]; then
                            print_success "Extracted Metadata URL: $AZURE_METADATA_URL"
                        fi
                        
                        if [[ -n "$AZURE_LOGIN_URL" ]] && [[ "$AZURE_LOGIN_URL" != "null" ]]; then
                            print_success "Extracted Login URL: $AZURE_LOGIN_URL"
                        fi
                        
                        if [[ -n "$cert_thumbprint" ]] && [[ "$cert_thumbprint" != "null" ]]; then
                            print_success "Certificate Thumbprint: $cert_thumbprint"
                        fi
                        
                        if [[ -n "$cert_expiration" ]] && [[ "$cert_expiration" != "null" ]]; then
                            print_success "Certificate Expiration: $cert_expiration"
                        fi
                    else
                        print_warning "Could not parse JSON output from Azure SSO script"
                    fi
                    
                    rm -f "${azure_json}.unwrapped"
                fi
            else
                print_warning "Could not find JSON end marker in script output"
            fi
        else
            print_warning "Could not find JSON output in script output"
        fi
        
        # Update deployment-config.json with Azure SSO URLs
        if [[ -n "$AZURE_METADATA_URL" ]] && [[ -n "$AZURE_LOGIN_URL" ]]; then
            print_status "Updating deployment-config.json with Azure SSO URLs..."
            local temp_config=$(mktemp)
            jq --arg metadata_url "$AZURE_METADATA_URL" --arg login_url "$AZURE_LOGIN_URL" '
                map(
                    if .ParameterKey == "AppFederationMetadataURL" then .ParameterValue = $metadata_url
                    elif .ParameterKey == "EntraIDLoginURL" then .ParameterValue = $login_url
                    else . end
                )
            ' deployment-config.json > "$temp_config"
            mv "$temp_config" deployment-config.json
            print_success "Updated deployment-config.json with Azure SSO URLs"
        fi
        
        # Clean up temp files
        rm -f "$azure_output" "$azure_json"
        
        return 0
    else
        print_error "Azure SSO setup failed"
        cat "$azure_output"
        rm -f "$azure_output" "$azure_json"
        return 1
    fi
}

# Function to deploy ALB stack
deploy_alb_stack() {
    print_status "Deploying ALB stack: $ALB_STACK_NAME"
    
    # Filter parameters for ALB template only
    local alb_params=$(mktemp)
    jq '[.[] | select(.ParameterKey | IN("ALBCertificateArn", "AzureAdminGroupID", "AzureUserGroupID", "AzureViewerGroupID", "AzureCustomGroupID", "EntraIDLoginURL", "AppFederationMetadataURL"))]' deployment-config.json > "$alb_params"
    
    # Override with Azure SSO URLs if they were set by setup_azure_sso
    if [[ -n "$AZURE_METADATA_URL" ]]; then
        print_status "Using Azure Metadata URL from SSO setup: $AZURE_METADATA_URL"
        local temp_params=$(mktemp)
        jq --arg metadata_url "$AZURE_METADATA_URL" '
            map(if .ParameterKey == "AppFederationMetadataURL" then .ParameterValue = $metadata_url else . end)
        ' "$alb_params" > "$temp_params"
        mv "$temp_params" "$alb_params"
    fi
    
    if [[ -n "$AZURE_LOGIN_URL" ]]; then
        print_status "Using Azure Login URL from SSO setup: $AZURE_LOGIN_URL"
        local temp_params=$(mktemp)
        jq --arg login_url "$AZURE_LOGIN_URL" '
            map(if .ParameterKey == "EntraIDLoginURL" then .ParameterValue = $login_url else . end)
        ' "$alb_params" > "$temp_params"
        mv "$temp_params" "$alb_params"
    fi
    
    # Add MWAAVPCStackName parameter
    local temp_params=$(mktemp)
    jq --arg vpc_stack "$VPC_STACK_NAME" '. + [{"ParameterKey": "MWAAVPCStackName", "ParameterValue": $vpc_stack}]' "$alb_params" > "$temp_params"
    
    # Check if stack already exists
    if aws cloudformation describe-stacks --stack-name "$ALB_STACK_NAME" &>/dev/null; then
        print_warning "Stack $ALB_STACK_NAME already exists. Updating..."
        aws cloudformation update-stack \
            --stack-name "$ALB_STACK_NAME" \
            --template-body file://02-alb.yml \
            --parameters file://"$temp_params" \
            --capabilities CAPABILITY_NAMED_IAM
    else
        print_status "Creating new stack: $ALB_STACK_NAME"
        aws cloudformation create-stack \
            --stack-name "$ALB_STACK_NAME" \
            --template-body file://02-alb.yml \
            --parameters file://"$temp_params" \
            --capabilities CAPABILITY_NAMED_IAM
    fi
    
    # Clean up temp files
    rm -f "$alb_params" "$temp_params"
    
    # Wait for stack completion
    if ! wait_for_stack "$ALB_STACK_NAME" "create/update"; then
        print_error "ALB stack deployment failed"
        exit 1
    fi
    
    print_success "ALB stack deployed successfully"
}

# Function to update Lambda environment variables
update_lambda_environment() {
    print_status "Updating Lambda function environment variables..."
    
    # Get Lambda function name from ALB stack
    local lambda_name=$(aws cloudformation describe-stacks \
        --stack-name "$ALB_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionName`].OutputValue' \
        --output text 2>/dev/null)
    
    if [[ -z "$lambda_name" || "$lambda_name" == "None" ]]; then
        print_error "Could not retrieve Lambda function name from ALB stack"
        return 1
    fi
    
    print_status "Lambda Function: $lambda_name"
    
    # Get current Lambda environment variables
    local current_env=$(aws lambda get-function-configuration \
        --function-name "$lambda_name" \
        --query 'Environment.Variables' \
        --output json 2>/dev/null)
    
    if [[ -z "$current_env" ]]; then
        print_error "Could not retrieve current Lambda environment variables"
        return 1
    fi
    
    # Extract existing variables
    local cognito_domain=$(echo "$current_env" | jq -r '.COGNITO_DOMAIN // ""')
    local aws_account_id=$(echo "$current_env" | jq -r '.AWS_ACCOUNT_ID // ""')
    local mwaa_env_name="$MWAA_ENV_NAME"  # Use VPC stack name as MWAA environment name
    local idp_login_uri=$(echo "$current_env" | jq -r '.IDP_LOGIN_URI // ""')
    local cognito_client_id=$(echo "$current_env" | jq -r '.COGNITO_CLIENT_ID // ""')
    local alb_cookie_name=$(echo "$current_env" | jq -r '.ALB_COOKIE_NAME // "AWSELBAuthSessionCookie"')
    
    # Get IAM role names (not ARNs) from ALB stack
    local admin_role=$(aws cloudformation describe-stacks \
        --stack-name "$ALB_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`MwaaAdminRoleArn`].OutputValue' \
        --output text 2>/dev/null | awk -F'/' '{print $NF}')
    
    local user_role=$(aws cloudformation describe-stacks \
        --stack-name "$ALB_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`MwaaUserRoleArn`].OutputValue' \
        --output text 2>/dev/null | awk -F'/' '{print $NF}')
    
    local viewer_role=$(aws cloudformation describe-stacks \
        --stack-name "$ALB_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`MwaaViewerRoleArn`].OutputValue' \
        --output text 2>/dev/null | awk -F'/' '{print $NF}')
    
    local custom_role=$(aws cloudformation describe-stacks \
        --stack-name "$ALB_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`MwaaCustomRoleArn`].OutputValue' \
        --output text 2>/dev/null | awk -F'/' '{print $NF}')
    
    # Get Azure AD group IDs from deployment config
    local admin_group=$(jq -r '.[] | select(.ParameterKey=="AzureAdminGroupID") | .ParameterValue' deployment-config.json)
    local user_group=$(jq -r '.[] | select(.ParameterKey=="AzureUserGroupID") | .ParameterValue' deployment-config.json)
    local viewer_group=$(jq -r '.[] | select(.ParameterKey=="AzureViewerGroupID") | .ParameterValue' deployment-config.json)
    local custom_group=$(jq -r '.[] | select(.ParameterKey=="AzureCustomGroupID") | .ParameterValue' deployment-config.json)
    
    # Extract DAG names from sample_dags folder
    print_status "Extracting DAG names from sample_dags folder..."
    local dag_files=$(ls sample_dags/*.py 2>/dev/null | xargs -n1 basename | sed 's/\.py$//')
    
    # Build specific_dags JSON array
    local specific_dags_json="[]"
    if [[ -n "$dag_files" ]]; then
        specific_dags_json=$(echo "$dag_files" | jq -R -s -c 'split("\n") | map(select(length > 0))')
        print_status "Found DAGs for custom role: $specific_dags_json"
    else
        print_warning "No DAG files found in sample_dags folder"
    fi
    
    # Build GROUP_TO_ROLE_MAP using jq for proper JSON encoding
    local group_to_role_map=$(jq -n \
        --arg admin_group "$admin_group" \
        --arg admin_role "$admin_role" \
        --arg user_group "$user_group" \
        --arg user_role "$user_role" \
        --arg viewer_group "$viewer_group" \
        --arg viewer_role "$viewer_role" \
        --arg custom_group "$custom_group" \
        --arg custom_role "$custom_role" \
        --argjson specific_dags "$specific_dags_json" \
        '[
            {"idp-group": $admin_group, "iam-role": $admin_role, "mwaa-role": "Admin"},
            {"idp-group": $user_group, "iam-role": $user_role, "mwaa-role": "User"},
            {"idp-group": $viewer_group, "iam-role": $viewer_role, "mwaa-role": "Viewer"},
            {"idp-group": $custom_group, "iam-role": $custom_role, "mwaa-role": "MWAARestrictedTest", "specific_dags": $specific_dags}
        ]' | jq -c .)
    
    # Build environment variables JSON
    local temp_env_file=$(mktemp)
    jq -n \
        --arg cognito_domain "$cognito_domain" \
        --arg aws_account_id "$aws_account_id" \
        --arg mwaa_env_name "$mwaa_env_name" \
        --arg group_to_role_map "$group_to_role_map" \
        --arg idp_login_uri "$idp_login_uri" \
        --arg cognito_client_id "$cognito_client_id" \
        --arg alb_cookie_name "$alb_cookie_name" \
        '{
            Variables: {
                COGNITO_DOMAIN: $cognito_domain,
                AWS_ACCOUNT_ID: $aws_account_id,
                Amazon_MWAA_ENV_NAME: $mwaa_env_name,
                GROUP_TO_ROLE_MAP: $group_to_role_map,
                IDP_LOGIN_URI: $idp_login_uri,
                COGNITO_CLIENT_ID: $cognito_client_id,
                ALB_COOKIE_NAME: $alb_cookie_name
            }
        }' > "$temp_env_file"
    
    print_status "Updating GROUP_TO_ROLE_MAP..."
    
    # Update Lambda environment variables using the JSON file
    aws lambda update-function-configuration \
        --function-name "$lambda_name" \
        --environment file://"$temp_env_file" \
        --query 'Environment.Variables.GROUP_TO_ROLE_MAP' \
        --output text > /dev/null
    
    # Clean up temp file
    rm -f "$temp_env_file"
    
    if [[ $? -eq 0 ]]; then
        print_success "Lambda environment variables updated successfully"
        print_status "GROUP_TO_ROLE_MAP configured with:"
        echo "  - Admin Group ($admin_group) → $admin_role → Admin"
        echo "  - User Group ($user_group) → $user_role → User"
        echo "  - Viewer Group ($viewer_group) → $viewer_role → Viewer"
        echo "  - Custom Group ($custom_group) → $custom_role → MWAARestrictedTest"
    else
        print_error "Failed to update Lambda environment variables"
        return 1
    fi
}

# Function to update Lambda function code
update_lambda_code() {
    print_status "Updating Lambda function code..."
    
    # Get Lambda function name from ALB stack
    local lambda_name=$(aws cloudformation describe-stacks \
        --stack-name "$ALB_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionName`].OutputValue' \
        --output text 2>/dev/null)
    
    if [[ -z "$lambda_name" || "$lambda_name" == "None" ]]; then
        print_error "Could not retrieve Lambda function name from ALB stack"
        return 1
    fi
    
    print_status "Lambda Function: $lambda_name"
    
    # Create temporary directory for packaging
    local temp_dir=$(mktemp -d)
    local package_dir="$temp_dir/package"
    mkdir -p "$package_dir"
    
    print_status "Installing dependencies..."
    
    # Check if Docker is available for building dependencies
    if command -v docker &> /dev/null; then
        print_status "Using Docker to build dependencies for Lambda runtime..."
        
        # Create a Dockerfile for building dependencies (without psycopg2-binary)
        cat > "$temp_dir/Dockerfile" << 'EOF'
FROM public.ecr.aws/lambda/python:3.11

# Install dependencies (psycopg2-binary comes from Lambda layer)
RUN pip install --target /package python-jose requests
EOF
        
        # Build the Docker image and extract the package
        docker build -t lambda-deps "$temp_dir" > /dev/null 2>&1
        
        if [[ $? -eq 0 ]]; then
            # Create a container and copy the package
            container_id=$(docker create lambda-deps)
            docker cp "$container_id:/package/." "$package_dir/"
            docker rm "$container_id" > /dev/null 2>&1
            docker rmi lambda-deps > /dev/null 2>&1
            
            print_success "Dependencies built successfully using Docker"
        else
            print_warning "Docker build failed, falling back to local pip install"
            python3 -m pip install --target "$package_dir" python-jose requests --quiet
        fi
    else
        print_warning "Docker not available, using local pip install"
        
        # Install dependencies to package directory (without psycopg2-binary)
        python3 -m pip install --target "$package_dir" python-jose requests --quiet
    fi
    
    if [[ $? -ne 0 ]]; then
        print_error "Failed to install Lambda dependencies"
        rm -rf "$temp_dir"
        return 1
    fi
    
    print_status "Creating deployment package..."
    
    # Copy Lambda function code and helper module
    cp lambda_auth/lambda_mwaa-authorizer.py "$package_dir/mwaa_authx_lambda_function.py"
    cp lambda_auth/mwaa_database_manager.py "$package_dir/mwaa_database_manager.py"
    
    # Create zip file
    local zip_file="$temp_dir/lambda_function.zip"
    (cd "$package_dir" && zip -r "$zip_file" . -q)
    
    if [[ $? -ne 0 ]]; then
        print_error "Failed to create deployment package"
        rm -rf "$temp_dir"
        return 1
    fi
    
    print_status "Uploading Lambda function code..."
    
    # Update Lambda function code
    aws lambda update-function-code \
        --function-name "$lambda_name" \
        --zip-file "fileb://$zip_file" \
        --output json > /dev/null
    
    if [[ $? -eq 0 ]]; then
        print_success "Lambda function code updated successfully"
        
        # Wait for Lambda to finish updating
        print_status "Waiting for Lambda function to be ready..."
        local count=0
        local max_attempts=30
        
        while [[ $count -lt $max_attempts ]]; do
            local state=$(aws lambda get-function \
                --function-name "$lambda_name" \
                --query 'Configuration.State' \
                --output text 2>/dev/null)
            
            if [[ "$state" == "Active" ]]; then
                print_success "Lambda function is ready"
                break
            elif [[ "$state" == "Failed" ]]; then
                print_error "Lambda function update failed"
                rm -rf "$temp_dir"
                return 1
            fi
            
            echo -n "."
            sleep 2
            ((count++))
        done
        
        if [[ $count -ge $max_attempts ]]; then
            print_warning "Timeout waiting for Lambda to be ready, but update was successful"
        fi
    else
        print_error "Failed to update Lambda function code"
        rm -rf "$temp_dir"
        return 1
    fi
    
    # Clean up
    rm -rf "$temp_dir"
}

# Function to display deployment summary
display_summary() {
    print_success "Deployment completed successfully!"
    echo
    print_status "Stack Information:"
    echo "  VPC Stack: $VPC_STACK_NAME"
    echo "  ALB Stack: $ALB_STACK_NAME"
    echo
    
    # Get ALB DNS name
    local alb_dns=$(aws cloudformation describe-stacks \
        --stack-name "$VPC_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`ApplicationLoadBalancerUrl`].OutputValue' \
        --output text 2>/dev/null)
    
    if [[ -n "$alb_dns" && "$alb_dns" != "None" ]]; then
        print_status "Access URL: https://$alb_dns/aws_mwaa/aws-console-sso"
    fi
    
    # Get MWAA environment name
    local mwaa_env=$(aws cloudformation describe-stacks \
        --stack-name "$VPC_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`MwaaEnvironmentName`].OutputValue' \
        --output text 2>/dev/null)
    
    if [[ -n "$mwaa_env" && "$mwaa_env" != "None" ]]; then
        print_status "MWAA Environment: $mwaa_env"
    fi
    
    # Get Lambda function name
    local lambda_name=$(aws cloudformation describe-stacks \
        --stack-name "$ALB_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionName`].OutputValue' \
        --output text 2>/dev/null)
    
    if [[ -n "$lambda_name" && "$lambda_name" != "None" ]]; then
        print_status "Lambda Function: $lambda_name"
    fi
    
    echo
    print_status "IAM Role ARNs (for Lambda GROUP_TO_ROLE_MAP):"
    
    # Get all role ARNs from ALB stack
    aws cloudformation describe-stacks \
        --stack-name "$ALB_STACK_NAME" \
        --query 'Stacks[0].Outputs[?contains(OutputKey, `Role`) && OutputKey!=`LambdaExecutionRole`].{Role:OutputKey,ARN:OutputValue}' \
        --output table 2>/dev/null || echo "  Could not retrieve role ARNs"
    
    echo
    print_status "Next Steps:"
    echo "1. Configure your Azure AD/Cognito identity provider"
    echo "2. Test the authentication flow"
    echo "3. Create custom MWAA roles using the create_role_glue_job_dag with config:"
    echo "   {\"stack_name\": \"$VPC_STACK_NAME\", \"source_role\": \"User\", \"target_role\": \"Restricted\"}"
    echo
    print_status "For troubleshooting, check the README.md file"
}

# Main deployment function
main() {
    echo "=========================================="
    echo "AWS MWAA Custom RBAC Solution Deployment"
    echo "=========================================="
    echo
    
    # Show which steps will run
    if [[ "$RUN_ALL" == true ]]; then
        print_status "Running all deployment steps"
    else
        print_status "Running selected steps:"
        [[ "$RUN_CREATE_CERT" == true ]] && echo "  ✓ SSL certificate creation"
        [[ "$RUN_VPC" == true ]] && echo "  ✓ VPC deployment"
        [[ "$RUN_AZURE_SSO" == true ]] && echo "  ✓ Azure SSO setup"
        [[ "$RUN_UPLOAD" == true ]] && echo "  ✓ File upload"
        [[ "$RUN_LAMBDA_LAYER" == true ]] && echo "  ✓ Lambda layer build and deployment"
        [[ "$RUN_ALB" == true ]] && echo "  ✓ ALB deployment"
        [[ "$RUN_LAMBDA_UPDATE" == true ]] && echo "  ✓ Lambda environment update"
        [[ "$RUN_LAMBDA_CODE_UPDATE" == true ]] && echo "  ✓ Lambda code update"
    fi
    echo
    
    # Check prerequisites
    check_prerequisites
    
    # Parse configuration
    parse_config
    
    # Create and import SSL certificate if flag is set or if running all steps
    # Skip if VPC stack already exists (certificate already created)
    if [[ "$RUN_CREATE_CERT" == true ]] || [[ "$RUN_ALL" == true ]]; then
        if aws cloudformation describe-stacks --stack-name "$VPC_STACK_NAME" &>/dev/null; then
            print_status "VPC stack $VPC_STACK_NAME already exists, skipping certificate creation"
        else
            create_and_import_certificate
        fi
    fi
    
    # Deploy VPC stack
    if [[ "$RUN_VPC" == true ]]; then
        # Check if stack exists
        if aws cloudformation describe-stacks --stack-name "$VPC_STACK_NAME" &>/dev/null; then
            print_warning "Stack $VPC_STACK_NAME already exists. Updating..."
            
            # Filter parameters for VPC template only
            local vpc_params=$(mktemp)
            jq '[.[] | select(.ParameterKey | IN("VpcCIDR", "PrivateSubnet1CIDR", "PrivateSubnet2CIDR", "PrivateSubnet3CIDR", "PublicSubnet1CIDR", "PublicSubnet2CIDR", "MWAAEnvironmentName"))]' deployment-config.json > "$vpc_params"
            
            aws cloudformation update-stack \
                --stack-name "$VPC_STACK_NAME" \
                --template-body file://01-vpc-mwaa.yml \
                --parameters file://"$vpc_params" \
                --capabilities CAPABILITY_NAMED_IAM 2>&1 | grep -v "No updates are to be performed" || true
            
            # Clean up temp file
            rm -f "$vpc_params"
            
            # Only wait if update was actually performed
            if aws cloudformation describe-stacks --stack-name "$VPC_STACK_NAME" --query 'Stacks[0].StackStatus' --output text 2>/dev/null | grep -q "UPDATE_IN_PROGRESS"; then
                if ! wait_for_stack "$VPC_STACK_NAME" "create/update"; then
                    print_error "VPC stack deployment failed"
                    exit 1
                fi
            else
                print_status "No updates needed for VPC stack"
            fi
        else
            deploy_vpc_stack
        fi
        
        # Setup Azure SSO after VPC deployment (Cognito is created in VPC stack)
        if [[ "$RUN_AZURE_SSO" == true ]]; then
            setup_azure_sso
        fi
    else
        print_status "Skipping VPC deployment"
        
        # If Azure SSO flag is set but VPC deployment is skipped, still run Azure SSO
        if [[ "$RUN_AZURE_SSO" == true ]] && [[ "$RUN_ALL" == false ]]; then
            setup_azure_sso
        fi
    fi
    
    # Upload files to S3
    if [[ "$RUN_UPLOAD" == true ]]; then
        # Get S3 bucket name (either from fresh deployment or existing stack)
        print_status "Getting S3 bucket name from VPC stack outputs..."
        S3_BUCKET_NAME=$(get_s3_bucket_name)
        print_success "S3 bucket name: $S3_BUCKET_NAME"
        upload_files_to_s3 "$S3_BUCKET_NAME"
    else
        print_status "Skipping file upload"
    fi
    
    # Deploy ALB stack
    if [[ "$RUN_ALB" == true ]]; then
        deploy_alb_stack
    else
        print_status "Skipping ALB deployment"
    fi
    
    # Build and deploy Lambda layer (after ALB stack completion)
    if [[ "$RUN_LAMBDA_LAYER" == true ]]; then
        build_and_deploy_lambda_layer
    else
        print_status "Skipping Lambda layer build and deployment"
    fi
    
    # Update Lambda environment variables after Lambda layer deployment
    if [[ "$RUN_ALB" == true ]]; then
        update_lambda_environment
    fi
    
    # Update Lambda environment variables only
    if [[ "$RUN_LAMBDA_UPDATE" == true ]]; then
        update_lambda_environment
    fi
    
    # Update Lambda function code only
    if [[ "$RUN_LAMBDA_CODE_UPDATE" == true ]]; then
        update_lambda_code
    fi
    
    # Display summary only if all steps were run
    if [[ "$RUN_ALL" == true ]]; then
        display_summary
    else
        echo
        print_success "Selected deployment steps completed successfully!"
        echo
        if [[ "$RUN_VPC" == true ]] && [[ "$RUN_UPLOAD" == false ]]; then
            print_warning "Next step: Run './deploy-stack.sh --upload' to upload files to S3"
        elif [[ "$RUN_UPLOAD" == true ]] && [[ "$RUN_ALB" == false ]]; then
            print_warning "Next step: Run './deploy-stack.sh --alb' to deploy ALB stack"
        elif [[ "$RUN_ALB" == true ]] && [[ "$RUN_LAMBDA_LAYER" == false ]]; then
            print_warning "Next step: Run './deploy-stack.sh --lambda-layer' to build and deploy Lambda layer"
        elif [[ "$RUN_VPC" == true ]] && [[ "$RUN_ALB" == false ]]; then
            print_warning "Next steps:"
            echo "  1. Run './deploy-stack.sh --upload' to upload files to S3"
            echo "  2. Run './deploy-stack.sh --alb' to deploy ALB stack"
            echo "  3. Run './deploy-stack.sh --lambda-layer' to build and deploy Lambda layer"
        fi
    fi
}

# Handle script interruption
trap 'print_error "Deployment interrupted"; exit 1' INT TERM

# Run main function
main "$@"