#!/bin/bash

################################################################################
# MWAA RBAC Custom Roles - CloudFormation Cleanup Script
#
# This script cleans up CloudFormation stacks and S3 resources
#
# Usage:
#   ./cleanup-stack.sh                                   # Run all cleanup steps sequentially
#   ./cleanup-stack.sh --vpc-stack my-vpc --alb-stack my-alb  # Custom stack names
#   ./cleanup-stack.sh --alb                             # Delete ALB stack only
#   ./cleanup-stack.sh --s3                              # Clean S3 bucket only
#   ./cleanup-stack.sh --vpc                             # Delete VPC stack only
#   ./cleanup-stack.sh --alb --s3                        # Delete ALB and clean S3
#   ./cleanup-stack.sh --force                           # Skip all confirmation prompts
#   ./cleanup-stack.sh --help                            # Show usage information
################################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Cleanup flags
DELETE_ALB=false
CLEAN_S3=false
DELETE_VPC=false
RUN_ALL=true
FORCE_MODE=false

# Stack names (defaults)
VPC_STACK_NAME=""
ALB_STACK_NAME=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --alb)
            DELETE_ALB=true
            RUN_ALL=false
            shift
            ;;
        --s3)
            CLEAN_S3=true
            RUN_ALL=false
            shift
            ;;
        --vpc)
            DELETE_VPC=true
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
        --force)
            FORCE_MODE=true
            shift
            ;;
        --help|-h)
            echo "MWAA RBAC Custom Roles - Cleanup Script"
            echo ""
            echo "Usage:"
            echo "  ./cleanup-stack.sh                                   # Run all cleanup steps sequentially"
            echo "  ./cleanup-stack.sh --vpc-stack my-vpc --alb-stack my-alb  # Custom stack names"
            echo "  ./cleanup-stack.sh --alb                             # Delete ALB stack only"
            echo "  ./cleanup-stack.sh --s3                              # Clean S3 bucket only"
            echo "  ./cleanup-stack.sh --vpc                             # Delete VPC stack only"
            echo "  ./cleanup-stack.sh --alb --s3                        # Delete ALB and clean S3"
            echo "  ./cleanup-stack.sh --force                           # Skip all confirmation prompts"
            echo "  ./cleanup-stack.sh --help                            # Show this help message"
            echo ""
            echo "Options:"
            echo "  --vpc-stack NAME    VPC stack name (default: mwaa-vpc)"
            echo "  --alb-stack NAME    ALB stack name (default: <vpc-stack>-alb)"
            echo ""
            echo "Steps:"
            echo "  --alb      Delete ALB and authentication stack"
            echo "  --s3       Clean S3 bucket (remove all objects)"
            echo "  --vpc      Delete VPC and MWAA infrastructure stack"
            echo "  --force    Skip confirmation prompts (can be combined with other flags)"
            echo ""
            echo "Examples:"
            echo "  # Full cleanup with confirmations"
            echo "  ./cleanup-stack.sh"
            echo ""
            echo "  # Full cleanup with custom stack names"
            echo "  ./cleanup-stack.sh --vpc-stack my-vpc --alb-stack my-alb"
            echo ""
            echo "  # Full cleanup without confirmations"
            echo "  ./cleanup-stack.sh --force"
            echo ""
            echo "  # Delete ALB, clean S3, then delete VPC with custom names"
            echo "  ./cleanup-stack.sh --vpc-stack my-vpc --alb-stack my-alb --alb"
            echo "  ./cleanup-stack.sh --vpc-stack my-vpc --s3"
            echo "  ./cleanup-stack.sh --vpc-stack my-vpc --vpc"
            echo ""
            echo "  # Delete ALB and clean S3 in one command"
            echo "  ./cleanup-stack.sh --alb --s3 --force"
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

# If no step flags provided, run all steps
if [[ "$RUN_ALL" == true ]]; then
    DELETE_ALB=true
    CLEAN_S3=true
    DELETE_VPC=true
fi

################################################################################
# Helper Functions
################################################################################

print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

# Check if AWS CLI is installed
check_aws_cli() {
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
}

# Check if jq is installed
check_jq() {
    if ! command -v jq &> /dev/null; then
        print_error "jq is not installed. Please install it first."
        exit 1
    fi
}

# Check if stack exists
stack_exists() {
    local stack_name=$1
    aws cloudformation describe-stacks --stack-name "$stack_name" &> /dev/null
    return $?
}

# Wait for stack deletion
wait_for_stack_deletion() {
    local stack_name=$1
    
    print_info "Waiting for stack $stack_name to be deleted..."
    
    local status=""
    local count=0
    local max_attempts=120  # 60 minutes max (30 second intervals)
    
    while [[ $count -lt $max_attempts ]]; do
        status=$(aws cloudformation describe-stacks --stack-name "$stack_name" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "STACK_NOT_FOUND")
        
        case $status in
            "DELETE_COMPLETE"|"STACK_NOT_FOUND")
                print_success "Stack $stack_name deleted successfully"
                return 0
                ;;
            "DELETE_FAILED")
                print_error "Stack $stack_name deletion failed"
                return 1
                ;;
            "DELETE_IN_PROGRESS")
                echo -n "."
                ;;
            *)
                print_warning "Unknown status: $status"
                ;;
        esac
        
        sleep 30
        ((count++))
    done
    
    print_error "Timeout waiting for stack $stack_name to be deleted"
    return 1
}

# Get stack output value
get_stack_output() {
    local stack_name=$1
    local output_key=$2
    
    aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --query "Stacks[0].Outputs[?OutputKey=='$output_key'].OutputValue" \
        --output text 2>/dev/null
}

# Empty S3 bucket
empty_s3_bucket() {
    local bucket_name=$1
    
    print_info "Checking S3 bucket: $bucket_name"
    
    # Check if bucket exists
    if ! aws s3 ls "s3://$bucket_name" &> /dev/null; then
        print_warning "Bucket $bucket_name does not exist or is not accessible"
        return 0
    fi
    
    # Count objects
    local object_count=$(aws s3 ls "s3://$bucket_name" --recursive 2>/dev/null | wc -l)
    
    if [ "$object_count" -eq 0 ]; then
        print_info "Bucket is already empty"
        return 0
    fi
    
    print_warning "Bucket contains $object_count objects"
    
    if [ "$FORCE_MODE" = false ]; then
        read -p "Do you want to delete all objects in the bucket? (y/n): " DELETE_OBJECTS
        if [ "$DELETE_OBJECTS" != "y" ]; then
            print_info "Skipping bucket cleanup"
            return 0
        fi
    fi
    
    print_info "Emptying S3 bucket..."
    
    # Delete all objects and versions
    aws s3 rm "s3://$bucket_name" --recursive
    
    # Delete all versions if versioning is enabled
    aws s3api list-object-versions \
        --bucket "$bucket_name" \
        --query 'Versions[].{Key:Key,VersionId:VersionId}' \
        --output json 2>/dev/null | \
    jq -r '.[] | "--key \"\(.Key)\" --version-id \"\(.VersionId)\""' | \
    xargs -I {} aws s3api delete-object --bucket "$bucket_name" {} 2>/dev/null || true
    
    # Delete all delete markers
    aws s3api list-object-versions \
        --bucket "$bucket_name" \
        --query 'DeleteMarkers[].{Key:Key,VersionId:VersionId}' \
        --output json 2>/dev/null | \
    jq -r '.[] | "--key \"\(.Key)\" --version-id \"\(.VersionId)\""' | \
    xargs -I {} aws s3api delete-object --bucket "$bucket_name" {} 2>/dev/null || true
    
    print_success "Bucket emptied"
}

# Delete stack
delete_stack() {
    local stack_name=$1
    
    if ! stack_exists "$stack_name"; then
        print_warning "Stack $stack_name does not exist"
        return 0
    fi
    
    print_info "Deleting stack: $stack_name"
    aws cloudformation delete-stack --stack-name "$stack_name"
    
    wait_for_stack_deletion "$stack_name"
}

# List all related stacks
list_stacks() {
    print_info "Searching for MWAA-related stacks..."
    
    aws cloudformation list-stacks \
        --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE UPDATE_ROLLBACK_COMPLETE \
        --query 'StackSummaries[?contains(StackName, `mwaa`) || contains(StackName, `alb`)].{Name:StackName,Status:StackStatus,Created:CreationTime}' \
        --output table 2>/dev/null || print_warning "No stacks found"
}

# Get stack names from config or use provided values
get_stack_names() {
    # Set default stack names if not provided
    if [[ -z "$VPC_STACK_NAME" ]]; then
        VPC_STACK_NAME="mwaa-vpc"
    fi
    
    if [[ -z "$ALB_STACK_NAME" ]]; then
        ALB_STACK_NAME="${VPC_STACK_NAME}-alb"
    fi
    
    print_info "VPC Stack Name: $VPC_STACK_NAME"
    print_info "ALB Stack Name: $ALB_STACK_NAME"
}

################################################################################
# Main Cleanup Flow
################################################################################

main() {
    print_header "MWAA RBAC Custom Roles - Cleanup"
    
    # Show which steps will run
    if [[ "$RUN_ALL" == true ]]; then
        print_info "Running all cleanup steps"
    else
        print_info "Running selected steps:"
        [[ "$DELETE_ALB" == true ]] && echo "  ✓ Delete ALB stack"
        [[ "$CLEAN_S3" == true ]] && echo "  ✓ Clean S3 bucket"
        [[ "$DELETE_VPC" == true ]] && echo "  ✓ Delete VPC stack"
    fi
    
    if [[ "$FORCE_MODE" == true ]]; then
        print_warning "Force mode enabled - skipping confirmations"
    fi
    echo
    
    # Pre-flight checks
    check_aws_cli
    check_jq
    
    # Get AWS region
    AWS_REGION=$(aws configure get region)
    if [ -z "$AWS_REGION" ]; then
        AWS_REGION="us-east-1"
        print_warning "No default region found, using us-east-1"
    fi
    print_info "Using AWS Region: $AWS_REGION"
    
    # Get stack names
    get_stack_names
    
    # List existing stacks
    list_stacks
    
    echo ""
    if [ "$FORCE_MODE" = false ] && [ "$RUN_ALL" = true ]; then
        print_warning "This will delete CloudFormation stacks and clean up S3 buckets."
        read -p "Do you want to continue? (y/n): " CONTINUE
        if [ "$CONTINUE" != "y" ]; then
            print_info "Cleanup cancelled"
            exit 0
        fi
    fi
    
    ############################################################################
    # Step 1: Delete ALB Stack
    ############################################################################
    if [ "$DELETE_ALB" = true ]; then
        print_header "Step 1: Delete ALB Stack"
        
        if [ "$FORCE_MODE" = false ] && [ "$RUN_ALL" = false ]; then
            read -p "Delete ALB stack '$ALB_STACK_NAME'? (y/n): " CONFIRM
            if [ "$CONFIRM" != "y" ]; then
                print_info "Skipping ALB stack deletion"
            else
                delete_stack "$ALB_STACK_NAME"
            fi
        else
            delete_stack "$ALB_STACK_NAME"
        fi
    else
        print_info "Skipping ALB stack deletion"
    fi
    
    ############################################################################
    # Step 2: Clean S3 Bucket
    ############################################################################
    if [ "$CLEAN_S3" = true ]; then
        print_header "Step 2: Clean S3 Bucket"
        
        if stack_exists "$VPC_STACK_NAME"; then
            S3_BUCKET=$(get_stack_output "$VPC_STACK_NAME" "MwaaS3BucketName")
            
            if [ -n "$S3_BUCKET" ]; then
                print_info "Found S3 bucket: $S3_BUCKET"
                empty_s3_bucket "$S3_BUCKET"
            else
                print_warning "Could not retrieve S3 bucket name from stack"
            fi
        else
            print_warning "VPC stack does not exist, cannot retrieve S3 bucket name"
        fi
    else
        print_info "Skipping S3 bucket cleanup"
    fi
    
    ############################################################################
    # Step 3: Delete VPC/MWAA Stack
    ############################################################################
    if [ "$DELETE_VPC" = true ]; then
        print_header "Step 3: Delete VPC/MWAA Stack"
        
        if [ "$FORCE_MODE" = false ]; then
            print_warning "This will delete the MWAA environment and VPC resources."
            print_warning "This operation cannot be undone!"
            read -p "Are you sure you want to delete '$VPC_STACK_NAME'? (yes/no): " CONFIRM
            if [ "$CONFIRM" != "yes" ]; then
                print_info "VPC stack deletion cancelled"
            else
                delete_stack "$VPC_STACK_NAME"
            fi
        else
            delete_stack "$VPC_STACK_NAME"
        fi
    else
        print_info "Skipping VPC stack deletion"
    fi
    
    ############################################################################
    # Completion
    ############################################################################
    print_header "Cleanup Complete!"
    
    echo ""
    if [[ "$RUN_ALL" == true ]]; then
        print_success "All resources have been cleaned up"
    else
        print_success "Selected cleanup steps completed"
        echo ""
        if [[ "$DELETE_ALB" == true ]] && [[ "$CLEAN_S3" == false ]]; then
            print_warning "Next step: Run './cleanup-stack.sh --s3' to clean S3 bucket"
        elif [[ "$CLEAN_S3" == true ]] && [[ "$DELETE_VPC" == false ]]; then
            print_warning "Next step: Run './cleanup-stack.sh --vpc' to delete VPC stack"
        elif [[ "$DELETE_ALB" == true ]] && [[ "$DELETE_VPC" == false ]]; then
            print_warning "Next steps:"
            echo "  1. Run './cleanup-stack.sh --s3' to clean S3 bucket"
            echo "  2. Run './cleanup-stack.sh --vpc' to delete VPC stack"
        fi
    fi
    echo ""
    
    print_info "To verify deletion:"
    echo "  aws cloudformation list-stacks --stack-status-filter DELETE_COMPLETE"
    echo ""
}

# Run main function
main "$@"
