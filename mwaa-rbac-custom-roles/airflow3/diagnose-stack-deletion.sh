#!/bin/bash

################################################################################
# MWAA Stack Deletion Diagnostic Script
#
# This script helps diagnose why a CloudFormation stack won't delete
################################################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

STACK_NAME="${1:-mwaa-af3-1}"

print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

print_header "CloudFormation Stack Deletion Diagnostics"
print_info "Stack Name: $STACK_NAME"

# Check if stack exists
print_header "1. Checking Stack Status"
STACK_STATUS=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "NOT_FOUND")

if [ "$STACK_STATUS" = "NOT_FOUND" ]; then
    print_success "Stack does not exist - already deleted"
    exit 0
fi

echo "Current Status: $STACK_STATUS"

if [ "$STACK_STATUS" = "DELETE_FAILED" ]; then
    print_error "Stack deletion failed"
    
    # Get failed resources
    print_header "2. Failed Resources"
    aws cloudformation describe-stack-resources --stack-name "$STACK_NAME" \
        --query 'StackResources[?ResourceStatus==`DELETE_FAILED`].{Resource:LogicalResourceId,Type:ResourceType,Reason:ResourceStatusReason}' \
        --output table
fi

# Check for stack exports being used by other stacks
print_header "3. Checking Export Dependencies"
EXPORTS=$(aws cloudformation list-exports --query "Exports[?ExportingStackId==\`$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].StackId' --output text)\`].Name" --output text 2>/dev/null || echo "")

if [ -n "$EXPORTS" ]; then
    print_warning "Stack has exports that might be imported by other stacks:"
    for export in $EXPORTS; do
        echo "  - $export"
        # Check which stacks import this
        aws cloudformation list-imports --export-name "$export" --output table 2>/dev/null || true
    done
else
    print_success "No export dependencies found"
fi

# Check S3 bucket
print_header "4. Checking S3 Bucket"
S3_BUCKET=$(aws cloudformation describe-stack-resources --stack-name "$STACK_NAME" \
    --query 'StackResources[?ResourceType==`AWS::S3::Bucket`].PhysicalResourceId' \
    --output text 2>/dev/null || echo "")

if [ -n "$S3_BUCKET" ]; then
    print_info "Found S3 Bucket: $S3_BUCKET"
    
    OBJECT_COUNT=$(aws s3 ls "s3://$S3_BUCKET" --recursive 2>/dev/null | wc -l || echo "0")
    if [ "$OBJECT_COUNT" -gt 0 ]; then
        print_error "Bucket contains $OBJECT_COUNT objects - must be emptied before deletion"
        echo "Run: aws s3 rm s3://$S3_BUCKET --recursive"
    else
        print_success "Bucket is empty"
    fi
else
    print_info "No S3 bucket found in stack"
fi

# Check MWAA environment
print_header "5. Checking MWAA Environment"
MWAA_ENV=$(aws cloudformation describe-stack-resources --stack-name "$STACK_NAME" \
    --query 'StackResources[?ResourceType==`AWS::MWAA::Environment`].PhysicalResourceId' \
    --output text 2>/dev/null || echo "")

if [ -n "$MWAA_ENV" ]; then
    print_info "Found MWAA Environment: $MWAA_ENV"
    
    MWAA_STATUS=$(aws mwaa get-environment --name "$MWAA_ENV" --query 'Environment.Status' --output text 2>/dev/null || echo "NOT_FOUND")
    echo "MWAA Status: $MWAA_STATUS"
    
    if [ "$MWAA_STATUS" != "NOT_FOUND" ] && [ "$MWAA_STATUS" != "DELETED" ]; then
        print_warning "MWAA environment still exists - deletion may take 20-30 minutes"
    fi
else
    print_info "No MWAA environment found in stack"
fi

# Check for VPC endpoints created by Lambda
print_header "6. Checking VPC Endpoints (created by Lambda)"
VPC_ID=$(aws cloudformation describe-stack-resources --stack-name "$STACK_NAME" \
    --query 'StackResources[?ResourceType==`AWS::EC2::VPC`].PhysicalResourceId' \
    --output text 2>/dev/null || echo "")

if [ -n "$VPC_ID" ]; then
    print_info "Found VPC: $VPC_ID"
    
    # Check for VPC endpoints with MWAA environment name
    MWAA_ENV_NAME=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
        --query 'Stacks[0].Parameters[?ParameterKey==`MWAAEnvironmentName`].ParameterValue' \
        --output text 2>/dev/null || echo "")
    
    if [ -n "$MWAA_ENV_NAME" ]; then
        print_info "Checking for VPC endpoints with name: $MWAA_ENV_NAME"
        
        VPCE_IDS=$(aws ec2 describe-vpc-endpoints \
            --filters "Name=vpc-id,Values=$VPC_ID" "Name=tag:Name,Values=${MWAA_ENV_NAME}-*" \
            --query 'VpcEndpoints[].VpcEndpointId' \
            --output text 2>/dev/null || echo "")
        
        if [ -n "$VPCE_IDS" ]; then
            print_error "Found VPC endpoints created by Lambda (not managed by CloudFormation):"
            for vpce_id in $VPCE_IDS; do
                VPCE_NAME=$(aws ec2 describe-vpc-endpoints --vpc-endpoint-ids "$vpce_id" \
                    --query 'VpcEndpoints[0].Tags[?Key==`Name`].Value' --output text)
                echo "  - $vpce_id ($VPCE_NAME)"
            done
            print_warning "These must be deleted manually:"
            for vpce_id in $VPCE_IDS; do
                echo "  aws ec2 delete-vpc-endpoints --vpc-endpoint-ids $vpce_id"
            done
        else
            print_success "No Lambda-created VPC endpoints found"
        fi
    fi
fi

# Check for ENIs
print_header "7. Checking Network Interfaces (ENIs)"
if [ -n "$VPC_ID" ]; then
    ENI_COUNT=$(aws ec2 describe-network-interfaces \
        --filters "Name=vpc-id,Values=$VPC_ID" "Name=description,Values=*MWAA*" \
        --query 'length(NetworkInterfaces)' \
        --output text 2>/dev/null || echo "0")
    
    if [ "$ENI_COUNT" -gt 0 ]; then
        print_warning "Found $ENI_COUNT MWAA-related network interfaces"
        print_info "These should be cleaned up automatically, but may take time"
        
        aws ec2 describe-network-interfaces \
            --filters "Name=vpc-id,Values=$VPC_ID" "Name=description,Values=*MWAA*" \
            --query 'NetworkInterfaces[].{ID:NetworkInterfaceId,Status:Status,Description:Description}' \
            --output table
    else
        print_success "No MWAA network interfaces found"
    fi
fi

# Provide recommendations
print_header "Recommendations"

if [ "$STACK_STATUS" = "CREATE_COMPLETE" ]; then
    print_error "Stack shows CREATE_COMPLETE but should be deleted"
    echo ""
    echo "This usually means:"
    echo "  1. The stack was never actually deleted (try deleting it now)"
    echo "  2. There's a different stack with a similar name"
    echo ""
    echo "To delete the stack:"
    echo "  aws cloudformation delete-stack --stack-name $STACK_NAME"
    echo ""
    echo "Or use the cleanup script:"
    echo "  ./cleanup-stack.sh --vpc-stack $STACK_NAME --force"
    
elif [ "$STACK_STATUS" = "DELETE_FAILED" ]; then
    echo "To retry deletion after fixing issues:"
    echo "  aws cloudformation delete-stack --stack-name $STACK_NAME"
    echo ""
    echo "Or to force delete (skip failed resources):"
    echo "  aws cloudformation delete-stack --stack-name $STACK_NAME --retain-resources <ResourceId>"
    
elif [ "$STACK_STATUS" = "DELETE_IN_PROGRESS" ]; then
    print_info "Stack deletion is in progress - this can take 20-30 minutes for MWAA"
    echo ""
    echo "Monitor progress:"
    echo "  aws cloudformation describe-stack-events --stack-name $STACK_NAME --max-items 20"
fi

print_header "Complete"
