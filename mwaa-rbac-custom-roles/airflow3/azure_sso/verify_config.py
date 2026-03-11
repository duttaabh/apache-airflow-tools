#!/usr/bin/env python3
"""
Verify the SAML configuration without using Portal UI
"""
import asyncio
from azure.identity.aio import DefaultAzureCredential
import httpx

async def verify_saml_config(sp_id):
    credential = DefaultAzureCredential()
    
    try:
        token_result = await credential.get_token("https://graph.microsoft.com/.default")
        access_token = token_result.token
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        async with httpx.AsyncClient() as client:
            # Get Service Principal
            sp_response = await client.get(
                f"https://graph.microsoft.com/v1.0/servicePrincipals/{sp_id}",
                headers=headers,
                timeout=30.0
            )
            
            if sp_response.status_code == 200:
                sp_data = sp_response.json()
                app_id = sp_data.get('appId')
                
                # Get Application
                app_response = await client.get(
                    f"https://graph.microsoft.com/v1.0/applications?$filter=appId eq '{app_id}'",
                    headers=headers,
                    timeout=30.0
                )
                
                if app_response.status_code == 200:
                    app_data = app_response.json()
                    if app_data.get('value'):
                        app = app_data['value'][0]
                        
                        print("=" * 80)
                        print("SAML Configuration Verification")
                        print("=" * 80)
                        print(f"\nService Principal Configuration:")
                        print(f"  SSO Mode: {sp_data.get('preferredSingleSignOnMode')}")
                        print(f"  Login URL: {sp_data.get('loginUrl')}")
                        print(f"  Reply URLs: {sp_data.get('replyUrls', [])}")
                        
                        print(f"\nApplication Configuration:")
                        print(f"  Identifier URIs: {app.get('identifierUris', [])}")
                        print(f"  Redirect URIs: {app.get('web', {}).get('redirectUris', [])}")
                        print(f"  Group Claims: {app.get('groupMembershipClaims')}")
                        
                        print("\n" + "=" * 80)
                        print("✓ Configuration retrieved successfully via API")
                        print("=" * 80)
    finally:
        await credential.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python verify_config.py <service-principal-id>")
        sys.exit(1)
    
    asyncio.run(verify_saml_config(sys.argv[1]))
