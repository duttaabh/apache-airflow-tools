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
RUN_ALB=false
RUN_LAMBDA_UPDATE=false
RUN_ALL=true

# Stack names (defaults)
VPC_STACK_NAME=""
ALB_STACK_NAME=""

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
            echo "  ./deploy-stack.sh --alb                              # Deploy ALB stack only"
            echo "  ./deploy-stack.sh --update-lambda                    # Update Lambda environment variables only"
            echo "  ./deploy-stack.sh --vpc --upload                     # Deploy VPC and upload files"
            echo "  ./deploy-stack.sh --help                             # Show this help message"
            echo ""
            echo "Options:"
            echo "  --vpc-stack NAME    VPC stack name (default: mwaa-vpc)"
            echo "  --alb-stack NAME    ALB stack name (default: <vpc-stack>-alb)"
            echo ""
            echo "Steps:"
            echo "  --vpc      Deploy VPC and MWAA infrastructure stack"
            echo "  --upload   Upload Lambda code and DAG files to S3"
            echo "  --alb      Deploy ALB and authentication stack"
            echo "  --update-lambda  Update Lambda environment variables only"
            echo ""
            echo "Examples:"
            echo "  # Full deployment with default stack names"
            echo "  ./deploy-stack.sh"
            echo ""
            echo "  # Full deployment with custom stack names"
            echo "  ./deploy-stack.sh --vpc-stack my-mwaa-vpc --alb-stack my-mwaa-alb"
            echo ""
            echo "  # Deploy VPC with custom name, then upload files, then deploy ALB"
            echo "  ./deploy-stack.sh --vpc-stack my-vpc --vpc"
            echo "  ./deploy-stack.sh --vpc-stack my-vpc --upload"
            echo "  ./deploy-stack.sh --vpc-stack my-vpc --alb-stack my-alb --alb"
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

# Function to parse deployment configuration
parse_config() {
    print_status "Parsing deployment configuration..."
    
    if [[ ! -f "deployment-config.json" ]]; then
        print_error "deployment-config.json not found"
        exit 1
    fi
    
    # Extract MWAA environment name from config
    MWAA_ENV_NAME=$(jq -r '.[] | select(.ParameterKey=="MWAAEnvironmentName") | .ParameterValue' deployment-config.json 2>/dev/null || echo "mwaa-env")
    
    # Set default stack names if not provided
    if [[ -z "$VPC_STACK_NAME" ]]; then
        VPC_STACK_NAME="mwaa-vpc"
    fi
    
    if [[ -z "$ALB_STACK_NAME" ]]; then
        ALB_STACK_NAME="${VPC_STACK_NAME}-alb"
    fi
    
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
    
    # Upload create_role_glue_job_dag.py (too large for CloudFormation inline)
    print_status "Uploading create_role_glue_job_dag.py..."
    aws s3 cp role_creation_dag/create_role_glue_job_dag.py "s3://$bucket_name/dags/create_role_glue_job_dag.py" \
        --content-type "text/x-python"
    
    print_success "Files uploaded successfully"
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

# Function to deploy ALB stack
deploy_alb_stack() {
    print_status "Deploying ALB stack: $ALB_STACK_NAME"
    
    # Filter parameters for ALB template only
    local alb_params=$(mktemp)
    jq '[.[] | select(.ParameterKey | IN("ALBCertificateArn", "AzureAdminGroupID", "AzureUserGroupID", "AzureViewerGroupID", "AzureCustomGroupID", "EntraIDLoginURL", "AppFederationMetadataURL"))]' deployment-config.json > "$alb_params"
    
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
    local mwaa_env_name=$(echo "$current_env" | jq -r '.Amazon_MWAA_ENV_NAME // ""')
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
    
    # Build GROUP_TO_ROLE_MAP JSON (using role names, not ARNs - Lambda will construct ARNs)
    # Create a proper JSON file to avoid shell escaping issues
    local temp_env_file=$(mktemp)
    cat > "$temp_env_file" <<EOF
{
  "Variables": {
    "COGNITO_DOMAIN": "$cognito_domain",
    "AWS_ACCOUNT_ID": "$aws_account_id",
    "Amazon_MWAA_ENV_NAME": "$mwaa_env_name",
    "GROUP_TO_ROLE_MAP": "[{\"idp-group\":\"$admin_group\",\"iam-role\":\"$admin_role\",\"mwaa-role\":\"Admin\"},{\"idp-group\":\"$user_group\",\"iam-role\":\"$user_role\",\"mwaa-role\":\"User\"},{\"idp-group\":\"$viewer_group\",\"iam-role\":\"$viewer_role\",\"mwaa-role\":\"Viewer\"},{\"idp-group\":\"$custom_group\",\"iam-role\":\"$custom_role\",\"mwaa-role\":\"MWAARestrictedTest\"}]",
    "IDP_LOGIN_URI": "$idp_login_uri",
    "COGNITO_CLIENT_ID": "$cognito_client_id",
    "ALB_COOKIE_NAME": "$alb_cookie_name"
  }
}
EOF
    
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
        [[ "$RUN_VPC" == true ]] && echo "  ✓ VPC deployment"
        [[ "$RUN_UPLOAD" == true ]] && echo "  ✓ File upload"
        [[ "$RUN_ALB" == true ]] && echo "  ✓ ALB deployment"
        [[ "$RUN_LAMBDA_UPDATE" == true ]] && echo "  ✓ Lambda environment update"
    fi
    echo
    
    # Check prerequisites
    check_prerequisites
    
    # Parse configuration
    parse_config
    
    # Deploy VPC stack
    if [[ "$RUN_VPC" == true ]]; then
        deploy_vpc_stack
    else
        print_status "Skipping VPC deployment"
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
        
        # Update Lambda environment variables after ALB deployment
        update_lambda_environment
    else
        print_status "Skipping ALB deployment"
    fi
    
    # Update Lambda environment variables only
    if [[ "$RUN_LAMBDA_UPDATE" == true ]]; then
        update_lambda_environment
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
        elif [[ "$RUN_VPC" == true ]] && [[ "$RUN_ALB" == false ]]; then
            print_warning "Next steps:"
            echo "  1. Run './deploy-stack.sh --upload' to upload files to S3"
            echo "  2. Run './deploy-stack.sh --alb' to deploy ALB stack"
        fi
    fi
}

# Handle script interruption
trap 'print_error "Deployment interrupted"; exit 1' INT TERM

# Run main function
main "$@"