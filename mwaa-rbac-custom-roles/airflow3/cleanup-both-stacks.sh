#!/bin/bash

################################################################################
# Complete MWAA Stack Cleanup
# Deletes both ALB and VPC stacks in the correct order
################################################################################

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

VPC_STACK="${1:-mwaa-af3-1}"
ALB_STACK="${2:-mwaa-alb-af3-1}"

print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

print_error() { echo -e "${RED}✗ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠ $1${NC}"; }
print_success() { echo -e "${GREEN}✓ $1${NC}"; }
print_info() { echo -e "${BLUE}ℹ $1${NC}"; }

print_header "Complete MWAA Stack Cleanup"
print_info "VPC Stack: $VPC_STACK"
print_info "ALB Stack: $ALB_STACK"

# Step 1: Delete ALB Stack (must be first due to exports)
print_header "Step 1: Delete ALB Stack"
ALB_STATUS=$(aws cloudformation describe-stacks --stack-name "$ALB_STACK" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "NOT_FOUND")

if [ "$ALB_STATUS" != "NOT_FOUND" ]; then
    print_info "ALB Stack Status: $ALB_STATUS"
    print_info "Deleting ALB stack..."
    aws cloudformation delete-stack --stack-name "$ALB_STACK"
    
    print_info "Waiting for ALB stack deletion..."
    aws cloudformation wait stack-delete-complete --stack-name "$ALB_STACK" 2>/dev/null || {
        print_warning "Wait command failed, checking status..."
        ALB_STATUS=$(aws cloudformation describe-stacks --stack-name "$ALB_STACK" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "NOT_FOUND")
        if [ "$ALB_STATUS" = "NOT_FOUND" ]; then
            print_success "ALB stack deleted"
        else
            print_error "ALB stack deletion failed with status: $ALB_STATUS"
            exit 1
        fi
    }
    print_success "ALB stack deleted"
else
    print_success "ALB stack already deleted"
fi

# Step 2: Delete Lambda-created VPC Endpoints
print_header "Step 2: Delete Lambda-created VPC Endpoints"
VPC_ID=$(aws cloudformation describe-stack-resources --stack-name "$VPC_STACK" \
    --query 'StackResources[?ResourceType==`AWS::EC2::VPC`].PhysicalResourceId' \
    --output text 2>/dev/null || echo "")

if [ -n "$VPC_ID" ]; then
    MWAA_ENV_NAME=$(aws cloudformation describe-stacks --stack-name "$VPC_STACK" \
        --query 'Stacks[0].Parameters[?ParameterKey==`MWAAEnvironmentName`].ParameterValue' \
        --output text 2>/dev/null || echo "")
    
    if [ -n "$MWAA_ENV_NAME" ]; then
        VPCE_IDS=$(aws ec2 describe-vpc-endpoints \
            --filters "Name=vpc-id,Values=$VPC_ID" "Name=tag:Name,Values=${MWAA_ENV_NAME}-*" \
            --query 'VpcEndpoints[].VpcEndpointId' \
            --output text 2>/dev/null || echo "")
        
        if [ -n "$VPCE_IDS" ]; then
            print_info "Deleting $(echo $VPCE_IDS | wc -w) VPC endpoints..."
            # Delete all at once
            aws ec2 delete-vpc-endpoints --vpc-endpoint-ids $VPCE_IDS
            print_success "VPC endpoints deleted"
            sleep 10
        else
            print_success "No Lambda-created VPC endpoints found"
        fi
    fi
fi

# Step 3: Empty S3 Bucket
print_header "Step 3: Empty S3 Bucket"
S3_BUCKET=$(aws cloudformation describe-stack-resources --stack-name "$VPC_STACK" \
    --query 'StackResources[?ResourceType==`AWS::S3::Bucket`].PhysicalResourceId' \
    --output text 2>/dev/null || echo "")

if [ -n "$S3_BUCKET" ]; then
    print_info "Emptying S3 bucket: $S3_BUCKET"
    aws s3 rm "s3://$S3_BUCKET" --recursive 2>/dev/null || true
    
    # Delete versions
    aws s3api list-object-versions --bucket "$S3_BUCKET" \
        --query 'Versions[].{Key:Key,VersionId:VersionId}' \
        --output json 2>/dev/null | \
    jq -r '.[]? | "--key \"\(.Key)\" --version-id \"\(.VersionId)\""' | \
    xargs -I {} aws s3api delete-object --bucket "$S3_BUCKET" {} 2>/dev/null || true
    
    # Delete markers
    aws s3api list-object-versions --bucket "$S3_BUCKET" \
        --query 'DeleteMarkers[].{Key:Key,VersionId:VersionId}' \
        --output json 2>/dev/null | \
    jq -r '.[]? | "--key \"\(.Key)\" --version-id \"\(.VersionId)\""' | \
    xargs -I {} aws s3api delete-object --bucket "$S3_BUCKET" {} 2>/dev/null || true
    
    print_success "S3 bucket emptied"
fi

# Step 4: Delete VPC Stack
print_header "Step 4: Delete VPC Stack"
VPC_STATUS=$(aws cloudformation describe-stacks --stack-name "$VPC_STACK" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "NOT_FOUND")

if [ "$VPC_STATUS" != "NOT_FOUND" ]; then
    print_info "VPC Stack Status: $VPC_STATUS"
    print_info "Deleting VPC stack..."
    aws cloudformation delete-stack --stack-name "$VPC_STACK"
    
    print_info "Waiting for VPC stack deletion (this may take 20-30 minutes for MWAA)..."
    
    COUNT=0
    MAX_ATTEMPTS=120
    
    while [ $COUNT -lt $MAX_ATTEMPTS ]; do
        VPC_STATUS=$(aws cloudformation describe-stacks --stack-name "$VPC_STACK" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "NOT_FOUND")
        
        if [ "$VPC_STATUS" = "NOT_FOUND" ]; then
            print_success "VPC stack deleted successfully!"
            exit 0
        elif [ "$VPC_STATUS" = "DELETE_FAILED" ]; then
            print_error "VPC stack deletion failed"
            aws cloudformation describe-stack-resources --stack-name "$VPC_STACK" \
                --query 'StackResources[?ResourceStatus==`DELETE_FAILED`].{Resource:LogicalResourceId,Reason:ResourceStatusReason}' \
                --output table
            exit 1
        elif [ "$VPC_STATUS" = "DELETE_IN_PROGRESS" ]; then
            echo -n "."
        fi
        
        sleep 30
        ((COUNT++))
    done
    
    print_error "Timeout waiting for stack deletion"
    exit 1
else
    print_success "VPC stack already deleted"
fi

print_header "Complete!"
