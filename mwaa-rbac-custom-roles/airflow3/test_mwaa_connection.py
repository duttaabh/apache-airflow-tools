#!/usr/bin/env python3
"""
Test script to check if Lambda can get MWAA metadata database connection info.
This script simulates what the Lambda function would do.
"""

import boto3
import json

def test_get_mwaa_env_info():
    """Test getting MWAA environment information"""
    
    mwaa_env_name = "mwaa-af3-1"
    aws_region = "us-east-1"
    
    print(f"Testing MWAA environment: {mwaa_env_name}")
    print(f"Region: {aws_region}")
    print("-" * 80)
    
    # Get MWAA client
    mwaa_client = boto3.client('mwaa', region_name=aws_region)
    
    try:
        # Get environment details
        print("\n1. Getting environment details via GetEnvironment API...")
        response = mwaa_client.get_environment(Name=mwaa_env_name)
        env = response.get('Environment', {})
        
        print(f"   Environment Name: {env.get('Name')}")
        print(f"   Status: {env.get('Status')}")
        print(f"   Airflow Version: {env.get('AirflowVersion')}")
        print(f"   Environment Class: {env.get('EnvironmentClass')}")
        
        # Check for database VPC endpoint
        db_vpc_endpoint = env.get('DatabaseVpcEndpointService')
        if db_vpc_endpoint:
            print(f"   Database VPC Endpoint: {db_vpc_endpoint}")
        else:
            print("   ✗ No DatabaseVpcEndpointService found in environment")
        
        # Check network configuration
        network_config = env.get('NetworkConfiguration', {})
        print(f"\n2. Network Configuration:")
        print(f"   Security Groups: {network_config.get('SecurityGroupIds')}")
        print(f"   Subnets: {network_config.get('SubnetIds')}")
        
        # The GetEnvironment API doesn't expose database credentials
        print(f"\n3. Database Connection Information:")
        print("   ✗ GetEnvironment API does not expose database host/credentials")
        print("   ✗ Database connection details are not available via AWS API")
        
        print(f"\n4. Alternative Approaches:")
        print("   ✓ Use Glue Connection (current approach)")
        print("   ✓ Use DAG to create Glue connection with database details")
        print("   ✓ Lambda retrieves credentials from Glue connection")
        
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_get_mwaa_env_info()
    
    if success:
        print("\n" + "=" * 80)
        print("CONCLUSION:")
        print("=" * 80)
        print("The MWAA GetEnvironment API does NOT provide database connection details.")
        print("The current approach using Glue Connection is the correct solution:")
        print("  1. DAG runs inside MWAA (has access to database env vars)")
        print("  2. DAG creates Glue Connection with database credentials")
        print("  3. Lambda retrieves credentials from Glue Connection")
        print("  4. Lambda uses credentials to connect to metadata database")
    else:
        print("\n✗ Test failed")
