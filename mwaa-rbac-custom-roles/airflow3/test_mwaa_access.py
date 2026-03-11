#!/usr/bin/env python3
"""
Test script to verify Lambda can access MWAA environment and metadata connection info.
"""

import boto3
import json

def test_mwaa_environment(env_name, region='us-east-1'):
    """Test if we can access MWAA environment details."""
    
    print(f"\n=== Testing MWAA Environment: {env_name} ===\n")
    
    mwaa_client = boto3.client('mwaa', region_name=region)
    
    try:
        # Get environment details
        print(f"1. Getting environment details...")
        response = mwaa_client.get_environment(Name=env_name)
        env = response['Environment']
        
        print(f"   ✓ Environment found: {env['Name']}")
        print(f"   ✓ Status: {env['Status']}")
        print(f"   ✓ Airflow Version: {env.get('AirflowVersion', 'N/A')}")
        print(f"   ✓ WebServer URL: {env.get('WebserverUrl', 'N/A')}")
        
        # Check if DatabaseVpcEndpointService is available
        db_vpc_endpoint = env.get('DatabaseVpcEndpointService')
        if db_vpc_endpoint:
            print(f"   ✓ Database VPC Endpoint: {db_vpc_endpoint}")
        else:
            print(f"   ✗ Database VPC Endpoint: Not available in API response")
        
        print(f"\n2. Checking Glue connection...")
        glue_client = boto3.client('glue', region_name=region)
        conn_name = f'{env_name}_metadata_conn'
        
        try:
            conn_response = glue_client.get_connection(Name=conn_name)
            connection = conn_response['Connection']
            
            print(f"   ✓ Glue connection found: {conn_name}")
            print(f"   ✓ Connection Type: {connection['ConnectionType']}")
            
            # Extract JDBC URL (without credentials)
            jdbc_url = connection['ConnectionProperties'].get('JDBC_CONNECTION_URL', 'N/A')
            # Mask the URL to not expose full details
            if 'postgresql://' in jdbc_url:
                host_part = jdbc_url.split('postgresql://')[1].split('/')[0]
                db_part = jdbc_url.split('/')[-1] if '/' in jdbc_url else 'unknown'
                print(f"   ✓ Database Host: {host_part}")
                print(f"   ✓ Database Name: {db_part}")
            else:
                print(f"   ✓ JDBC URL: {jdbc_url}")
                
        except glue_client.exceptions.EntityNotFoundException:
            print(f"   ✗ Glue connection '{conn_name}' not found")
            print(f"   → Run the create_glue_connection DAG to create it")
        
        print(f"\n3. Testing web login token generation...")
        try:
            token_response = mwaa_client.create_web_login_token(Name=env_name)
            print(f"   ✓ Web login token generated successfully")
            print(f"   ✓ Token length: {len(token_response['WebToken'])} characters")
        except Exception as e:
            print(f"   ✗ Failed to generate web login token: {e}")
        
        print(f"\n=== Test Complete ===\n")
        return True
        
    except mwaa_client.exceptions.ResourceNotFoundException:
        print(f"   ✗ Environment '{env_name}' not found")
        return False
    except Exception as e:
        print(f"   ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python test_mwaa_access.py <environment-name> [region]")
        print("Example: python test_mwaa_access.py af3-1 us-east-1")
        sys.exit(1)
    
    env_name = sys.argv[1]
    region = sys.argv[2] if len(sys.argv) > 2 else 'us-east-1'
    
    success = test_mwaa_environment(env_name, region)
    sys.exit(0 if success else 1)
