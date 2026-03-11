#!/bin/bash

################################################################################
# MWAA Stack Force Deletion Script
#
# This script forcefully deletes a CloudFormation stack by:
# 1. Emptying S3 buckets
# 2. Deleting Lambda-created VPC endpoints
# 3. Deleting the MWAA environment manually if needed
# 4. Retrying stack deletion
################################################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

STACK_NAME="${1:-mwaa-af3-1}"
FORCE_MODE="${2:-false}"

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

confirm() {
    if [ "$FORCE_MODE" = "true" ]; then
        return 0
    fi
    read -p "$1 (y/n): " -n 1 -r
    echo
    [[ $REPLY =~ ^[Yy]$ ]]
}

print_header "Force Delete CloudFormation Stack"
print_warning "Stack Name: $STACK_NAME"

if [ "$FORCE_MODE" = "true" ]; then
    print_warning "Running in FORCE MODE - no confirmations"
fi

# Check if stack exists
STACK_STATUS=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "NOT_FOUND")

if [ "$STACK_STATUS" = "NOT_FOUND" ]; then
    print_success "Stack does not exist - nothing to delete"
    exit 0
fi

print_info "Current Status: $STACK_STATUS"

# Step 1: Empty S3 Bucket
print_header "Step 1: Empty S3 Bucket"
S3_BUCKET=$(aws cloudformation describe-stack-resources --stack-name "$STACK_NAME" \
    --query 'StackResources[?ResourceType==`AWS::S3::Bucket`].PhysicalResourceId' \
    --output text 2>/dev/null || echo "")

if [ -n "$S3_BUCKET" ]; then
    print_info "Found S3 Bucket: $S3_BUCKET"
    
    OBJECT_COUNT=$(aws s3 ls "s3://$S3_BUCKET" --recursive 2>/dev/null | wc -l || echo "0")
    if [ "$OBJECT_COUNT" -gt 0 ]; then
        print_warning "Bucket contains $OBJECT_COUNT objects"
        
        if confirm "Empty S3 bucket?"; then
            print_info "Emptying bucket..."
            aws s3 rm "s3://$S3_BUCKET" --recursive
            
            # Delete versions if versioning enabled
            aws s3api list-object-versions --bucket "$S3_BUCKET" \
                --query 'Versions[].{Key:Key,VersionId:VersionId}' \
                --output json 2>/dev/null | \
            jq -r '.[] | "--key \"\(.Key)\" --version-id \"\(.VersionId)\""' | \
            xargs -I {} aws s3api delete-object --bucket "$S3_BUCKET" {} 2>/dev/null || true
            
            # Delete delete markers
            aws s3api list-object-versions --bucket "$S3_BUCKET" \
                --query 'DeleteMarkers[].{Key:Key,VersionId:VersionId}' \
                --output json 2>/dev/null | \
            jq -r '.[] | "--key \"\(.Key)\" --version-id \"\(.VersionId)\""' | \
            xargs -I {} aws s3api delete-object --bucket "$S3_BUCKET" {} 2>/dev/null || true
            
            print_success "Bucket emptied"
        fi
    else
        print_success "Bucket is already empty"
    fi
else
    print_info "No S3 bucket found"
fi

# Step 2: Delete Lambda-created VPC Endpoints
print_header "Step 2: Delete Lambda-created VPC Endpoints"
VPC_ID=$(aws cloudformation describe-stack-resources --stack-name "$STACK_NAME" \
    --query 'StackResources[?ResourceType==`AWS::EC2::VPC`].PhysicalResourceId' \
    --output text 2>/dev/null || echo "")

if [ -n "$VPC_ID" ]; then
    MWAA_ENV_NAME=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
        --query 'Stacks[0].Parameters[?ParameterKey==`MWAAEnvironmentName`].ParameterValue' \
        --output text 2>/dev/null || echo "")
    
    if [ -n "$MWAA_ENV_NAME" ]; then
        VPCE_IDS=$(aws ec2 describe-vpc-endpoints \
            --filters "Name=vpc-id,Values=$VPC_ID" "Name=tag:Name,Values=${MWAA_ENV_NAME}-*" \
            --query 'VpcEndpoints[].VpcEndpointId' \
            --output text 2>/dev/null || echo "")
        
        if [ -n "$VPCE_IDS" ]; then
            print_warning "Found Lambda-created VPC endpoints:"
            for vpce_id in $VPCE_IDS; do
                VPCE_NAME=$(aws ec2 describe-vpc-endpoints --vpc-endpoint-ids "$vpce_id" \
                    --query 'VpcEndpoints[0].Tags[?Key==`Name`].Value' --output text)
                echo "  - $vpce_id ($VPCE_NAME)"
            done
            
            if confirm "Delete these VPC endpoints?"; then
                for vpce_id in $VPCE_IDS; do
                    print_info "Deleting $vpce_id..."
                    aws ec2 delete-vpc-endpoints --vpc-endpoint-ids "$vpce_id" || print_error "Failed to delete $vpce_id"
                done
                print_success "VPC endpoints deleted"
                
                # Wait a bit for deletion to propagate
                print_info "Waiting 10 seconds for deletion to propagate..."
                sleep 10
            fi
        else
            print_success "No Lambda-created VPC endpoints found"
        fi
    fi
fi

# Step 3: Check MWAA Environment
print_header "Step 3: Check MWAA Environment"
MWAA_ENV=$(aws cloudformation describe-stack-resources --stack-name "$STACK_NAME" \
    --query 'StackResources[?ResourceType==`AWS::MWAA::Environment`].PhysicalResourceId' \
    --output text 2>/dev/null || echo "")

if [ -n "$MWAA_ENV" ]; then
    MWAA_STATUS=$(aws mwaa get-environment --name "$MWAA_ENV" --query 'Environment.Status' --output text 2>/dev/null || echo "NOT_FOUND")
    
    if [ "$MWAA_STATUS" != "NOT_FOUND" ] && [ "$MWAA_STATUS" != "DELETED" ]; then
        print_warning "MWAA Environment Status: $MWAA_STATUS"
        
        if [ "$MWAA_STATUS" = "AVAILABLE" ] || [ "$MWAA_STATUS" = "CREATE_FAILED" ]; then
            if confirm "Delete MWAA environment manually?"; then
                print_info "Deleting MWAA environment..."
                aws mwaa delete-environment --name "$MWAA_ENV"
                print_warning "MWAA deletion initiated - this takes 20-30 minutes"
                
                if confirm "Wait for MWAA deletion to complete?"; then
                    print_info "Waiting for MWAA deletion..."
                    while true; do
                        MWAA_STATUS=$(aws mwaa get-environment --name "$MWAA_ENV" --query 'Environment.Status' --output text 2>/dev/null || echo "NOT_FOUND")
                        if [ "$MWAA_STATUS" = "NOT_FOUND" ] || [ "$MWAA_STATUS" = "DELETED" ]; then
                            print_success "MWAA environment deleted"
                            break
                        fi
                        echo -n "."
                        sleep 30
                    done
                fi
            fi
        elif [ "$MWAA_STATUS" = "DELETING" ]; then
            print_info "MWAA is already being deleted"
            if confirm "Wait for MWAA deletion to complete?"; then
                print_info "Waiting for MWAA deletion..."
                while true; do
                    MWAA_STATUS=$(aws mwaa get-environment --name "$MWAA_ENV" --query 'Environment.Status' --output text 2>/dev/null || echo "NOT_FOUND")
                    if [ "$MWAA_STATUS" = "NOT_FOUND" ] || [ "$MWAA_STATUS" = "DELETED" ]; then
                        print_success "MWAA environment deleted"
                        break
                    fi
                    echo -n "."
                    sleep 30
                done
            fi
        fi
    else
        print_success "MWAA environment already deleted"
    fi
fi

# Step 4: Delete CloudFormation Stack
print_header "Step 4: Delete CloudFormation Stack"

if [ "$STACK_STATUS" = "DELETE_FAILED" ]; then
    print_warning "Stack is in DELETE_FAILED state"
    
    # Get failed resources
    FAILED_RESOURCES=$(aws cloudformation describe-stack-resources --stack-name "$STACK_NAME" \
        --query 'StackResources[?ResourceStatus==`DELETE_FAILED`].LogicalResourceId' \
        --output text 2>/dev/null || echo "")
    
    if [ -n "$FAILED_RESOURCES" ]; then
        print_error "Failed resources:"
        for resource in $FAILED_RESOURCES; do
            echo "  - $resource"
        done
        
        if confirm "Retry deletion (will skip failed resources if they still fail)?"; then
            print_info "Retrying stack deletion..."
            aws cloudformation delete-stack --stack-name "$STACK_NAME"
        fi
    fi
elif [ "$STACK_STATUS" = "CREATE_COMPLETE" ] || [ "$STACK_STATUS" = "UPDATE_COMPLETE" ]; then
    if confirm "Delete CloudFormation stack?"; then
        print_info "Deleting stack..."
        aws cloudformation delete-stack --stack-name "$STACK_NAME"
        print_success "Stack deletion initiated"
    fi
elif [ "$STACK_STATUS" = "DELETE_IN_PROGRESS" ]; then
    print_info "Stack deletion already in progress"
else
    print_warning "Stack is in $STACK_STATUS state"
fi

# Step 5: Monitor deletion
print_header "Step 5: Monitor Deletion"

if confirm "Wait for stack deletion to complete?"; then
    print_info "Monitoring stack deletion..."
    
    COUNT=0
    MAX_ATTEMPTS=120  # 60 minutes
    
    while [ $COUNT -lt $MAX_ATTEMPTS ]; do
        STACK_STATUS=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "NOT_FOUND")
        
        if [ "$STACK_STATUS" = "NOT_FOUND" ]; then
            print_success "Stack deleted successfully!"
            exit 0
        elif [ "$STACK_STATUS" = "DELETE_FAILED" ]; then
            print_error "Stack deletion failed"
            
            # Show failed resources
            aws cloudformation describe-stack-resources --stack-name "$STACK_NAME" \
                --query 'StackResources[?ResourceStatus==`DELETE_FAILED`].{Resource:LogicalResourceId,Type:ResourceType,Reason:ResourceStatusReason}' \
                --output table
            
            print_warning "You may need to manually delete failed resources and retry"
            exit 1
        elif [ "$STACK_STATUS" = "DELETE_IN_PROGRESS" ]; then
            echo -n "."
        else
            print_warning "Unexpected status: $STACK_STATUS"
        fi
        
        sleep 30
        ((COUNT++))
    done
    
    print_error "Timeout waiting for stack deletion"
    exit 1
fi

print_header "Complete"
print_info "Stack deletion initiated. Monitor with:"
echo "  aws cloudformation describe-stack-events --stack-name $STACK_NAME --max-items 20"
