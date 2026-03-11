#!/usr/bin/env python3
"""
Create Non-Gallery SAML Enterprise Application (Portal Workflow Replication)

This script replicates the exact Azure Portal workflow:
1. Create Non-Gallery Enterprise Application (like Portal's "Create your own application")
2. Configure SAML SSO mode
3. Set all SAML configuration (Entity ID, Reply URL, Sign-on URL, Group Claims)
4. Provide Metadata URL for AWS Cognito

Usage:
    python create_nongallery_saml_app.py \\
        --name "MWAA-Cognito-SAML" \\
        --entity-id "urn:amazon:cognito:sp:us-east-1_XXXXX" \\
        --reply-url "https://your-domain.auth.us-east-1.amazoncognito.com/saml2/idpresponse" \\
        --sign-on-url "https://your-alb.elb.amazonaws.com"
"""

import asyncio
import argparse
import sys
import json
from azure.identity.aio import DefaultAzureCredential
import httpx


async def create_nongallery_saml_app(app_name, entity_id, reply_url, sign_on_url, stack_name=None):
    """
    Create a non-gallery SAML Enterprise Application (Portal workflow).

    This replicates: Portal → Enterprise Apps → New → Create your own → Non-gallery → SAML

    Args:
        app_name: Display name for the application
        entity_id: SAML Entity ID (Identifier)
        reply_url: SAML Reply URL (Assertion Consumer Service URL)
        sign_on_url: SAML Sign-on URL
        stack_name: Optional stack name to append to app name for uniqueness

    Returns:
        dict: Application details
    """

    # Append stack name to application name if provided
    if stack_name:
        full_app_name = f"{app_name}-{stack_name}"
    else:
        full_app_name = app_name

    print("=" * 80)
    print("Create Non-Gallery SAML Enterprise Application")
    print("=" * 80)
    print(f"\nConfiguration:")
    print(f"  App Name:        {full_app_name}")
    if stack_name:
        print(f"  Stack Name:      {stack_name}")
    print(f"  Entity ID:       {entity_id}")
    print(f"  Reply URL:       {reply_url}")
    print(f"  Sign-on URL:     {sign_on_url}")
    print()

    # Authenticate
    print("Authenticating with Azure AD...")
    credential = DefaultAzureCredential()

    try:
        # Get access token
        token_result = await credential.get_token("https://graph.microsoft.com/.default")
        access_token = token_result.token
        print("✓ Authentication successful\n")

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient() as client:
            # Step 1: Get tenant information
            print("Retrieving tenant information...")
            org_response = await client.get(
                "https://graph.microsoft.com/v1.0/organization",
                headers=headers,
                timeout=30.0
            )

            if org_response.status_code == 200:
                org_data = org_response.json()
                if org_data.get('value') and len(org_data['value']) > 0:
                    tenant_id = org_data['value'][0]['id']
                    tenant_name = org_data['value'][0].get('displayName', 'Unknown')
                    print(f"✓ Connected to tenant: {tenant_name}")
                    print(f"  Tenant ID: {tenant_id}\n")
                else:
                    tenant_id = "unknown"
            else:
                tenant_id = "unknown"

            # Step 2: Check for and delete existing applications with same Entity ID
            print("Step 1/7: Checking for existing applications...")

            # Search for applications with the same identifierUri
            search_response = await client.get(
                f"https://graph.microsoft.com/v1.0/applications?$filter=identifierUris/any(uri:uri eq '{entity_id}')",
                headers=headers,
                timeout=30.0
            )

            if search_response.status_code == 200:
                search_data = search_response.json()
                existing_apps = search_data.get('value', [])

                if existing_apps:
                    print(f"Found {len(existing_apps)} existing application(s) with same Entity ID")

                    for existing_app in existing_apps:
                        existing_app_id = existing_app['id']
                        existing_app_client_id = existing_app['appId']
                        existing_app_name = existing_app['displayName']

                        print(f"  Deleting: {existing_app_name}")
                        print(f"    Application ID: {existing_app_id}")

                        # First, find and delete the associated Service Principal
                        sp_search_response = await client.get(
                            f"https://graph.microsoft.com/v1.0/servicePrincipals?$filter=appId eq '{existing_app_client_id}'",
                            headers=headers,
                            timeout=30.0
                        )

                        if sp_search_response.status_code == 200:
                            sp_search_data = sp_search_response.json()
                            existing_sps = sp_search_data.get('value', [])

                            for existing_sp in existing_sps:
                                sp_delete_response = await client.delete(
                                    f"https://graph.microsoft.com/v1.0/servicePrincipals/{existing_sp['id']}",
                                    headers=headers,
                                    timeout=30.0
                                )

                                if sp_delete_response.status_code in [204, 200]:
                                    print(f"    ✓ Deleted Service Principal (Enterprise App)")
                                else:
                                    print(f"    ⚠ Could not delete Service Principal (status {sp_delete_response.status_code})")

                        # Then delete the Application
                        app_delete_response = await client.delete(
                            f"https://graph.microsoft.com/v1.0/applications/{existing_app_id}",
                            headers=headers,
                            timeout=30.0
                        )

                        if app_delete_response.status_code in [204, 200]:
                            print(f"    ✓ Deleted App Registration")
                        else:
                            print(f"    ⚠ Could not delete App Registration (status {app_delete_response.status_code})")

                    print(f"\n✓ Cleaned up {len(existing_apps)} existing application(s)\n")

                    # Small delay to ensure deletion propagates
                    await asyncio.sleep(2)
                else:
                    print(f"No existing applications found with Entity ID\n")

            # Step 3: Create minimal Application Registration
            # This is the backend for the Enterprise App - keep it minimal for SAML
            print("Step 2/7: Creating App Registration (backend)...")

            app_payload = {
                "displayName": full_app_name,
                "signInAudience": "AzureADMyOrg"
                # NO web, api, or other OAuth/OIDC properties!
            }

            app_response = await client.post(
                "https://graph.microsoft.com/v1.0/applications",
                json=app_payload,
                headers=headers,
                timeout=30.0
            )

            if app_response.status_code != 201:
                print(f"✗ Failed to create application (status {app_response.status_code})")
                print(f"  Response: {app_response.text}")
                return None

            app_data = app_response.json()
            app_id = app_data['id']
            app_client_id = app_data['appId']

            print(f"✓ Created App Registration")
            print(f"  Application ID: {app_id}")
            print(f"  Client ID: {app_client_id}\n")

            # Step 4: Create Service Principal (Enterprise Application)
            # This is what users see in "Enterprise Applications"
            print("Step 3/7: Creating Enterprise Application...")

            sp_payload = {
                "appId": app_client_id,
                "tags": [
                    "WindowsAzureActiveDirectoryIntegratedApp",
                    "WindowsAzureActiveDirectoryCustomSingleSignOnApplication"
                ],
                "preferredSingleSignOnMode": "saml"  # Set SAML mode immediately
            }

            sp_response = await client.post(
                "https://graph.microsoft.com/v1.0/servicePrincipals",
                json=sp_payload,
                headers=headers,
                timeout=30.0
            )

            if sp_response.status_code != 201:
                print(f"✗ Failed to create Enterprise Application (status {sp_response.status_code})")
                print(f"  Response: {sp_response.text}")
                return None

            sp_data = sp_response.json()
            sp_id = sp_data['id']

            print(f"✓ Created Enterprise Application with SAML SSO mode")
            print(f"  Service Principal ID: {sp_id}\n")

            # Small delay for propagation
            await asyncio.sleep(2)

            # Step 5: Configure Basic SAML Configuration (Entity ID, Reply URL, and Sign-on URL)
            print("Step 4/7: Configuring SAML Basic Configuration...")

            # Set identifierUris and web.redirectUris on the Application
            basic_saml_payload = {
                "identifierUris": [entity_id],
                "web": {
                    "redirectUris": [reply_url]
                }
            }

            basic_saml_response = await client.patch(
                f"https://graph.microsoft.com/v1.0/applications/{app_id}",
                json=basic_saml_payload,
                headers=headers,
                timeout=30.0
            )

            if basic_saml_response.status_code in [200, 204]:
                print(f"✓ Configured Entity ID: {entity_id}")
                print(f"✓ Configured Reply URL: {reply_url}")
            else:
                print(f"⚠ Could not configure Basic SAML (status {basic_saml_response.status_code})")
                print(f"  Response: {basic_saml_response.text}")

            # Set Sign-on URL on the Service Principal
            await asyncio.sleep(1)
            sign_on_payload = {
                "loginUrl": sign_on_url
            }

            sign_on_response = await client.patch(
                f"https://graph.microsoft.com/v1.0/servicePrincipals/{sp_id}",
                json=sign_on_payload,
                headers=headers,
                timeout=30.0
            )

            if sign_on_response.status_code in [200, 204]:
                print(f"✓ Configured Sign-on URL: {sign_on_url}\n")
            else:
                print(f"⚠ Could not configure Sign-on URL (status {sign_on_response.status_code})")
                print(f"  You'll need to set this manually in Azure Portal\n")

            # Step 6: Configure Attributes & Claims (Group Membership)
            print("Step 5/7: Configuring Attributes & Claims...")

            claims_payload = {
                "groupMembershipClaims": "SecurityGroup"
            }

            claims_response = await client.patch(
                f"https://graph.microsoft.com/v1.0/applications/{app_id}",
                json=claims_payload,
                headers=headers,
                timeout=30.0
            )

            if claims_response.status_code in [200, 204]:
                print(f"✓ Configured Security Group claims in token\n")
            else:
                print(f"⚠ Could not configure claims (status {claims_response.status_code})\n")

            # Step 6.5: Generate SAML Token Signing Certificate
            print("Step 6/7: Generating SAML token signing certificate...")

            try:
                # Use the addTokenSigningCertificate action to generate a self-signed certificate
                cert_gen_response = await client.post(
                    f"https://graph.microsoft.com/v1.0/servicePrincipals/{sp_id}/addTokenSigningCertificate",
                    json={
                        "displayName": "CN=Microsoft Azure Federated SSO Certificate",
                        "endDateTime": None  # Use default expiration (3 years)
                    },
                    headers=headers,
                    timeout=30.0
                )

                if cert_gen_response.status_code == 200:
                    cert_gen_data = cert_gen_response.json()
                    print(f"✓ Generated SAML signing certificate")

                    # Extract certificate details from response
                    if 'thumbprint' in cert_gen_data:
                        print(f"  Thumbprint: {cert_gen_data['thumbprint']}")
                    if 'endDateTime' in cert_gen_data:
                        print(f"  Expiration: {cert_gen_data['endDateTime']}")
                else:
                    print(f"⚠ Could not generate certificate (status {cert_gen_response.status_code})")
                    print(f"  Response: {cert_gen_response.text}")
                    print(f"  Azure may auto-generate on first use")

                # Small delay for certificate propagation
                await asyncio.sleep(2)

            except Exception as e:
                print(f"⚠ Error generating certificate: {e}")
                print(f"  Azure may auto-generate on first use")

            # Step 7: Verify certificate and generate URLs
            print("\nStep 7/7: Verifying SAML signing certificate and generating URLs...")

            metadata_url = f"https://login.microsoftonline.com/{tenant_id}/federationmetadata/2007-06/federationmetadata.xml?appid={app_client_id}"
            login_url = f"https://login.microsoftonline.com/{tenant_id}/saml2"
            entra_identifier = f"https://sts.windows.net/{tenant_id}/"
            logout_url = f"https://login.microsoftonline.com/{tenant_id}/saml2"

            # Retrieve Service Principal's key credentials to get certificate details
            certificate_thumbprint = None
            certificate_data = None
            certificate_expiration = None
            certificate_status = "Unknown"

            try:
                # Get Service Principal details including keyCredentials
                sp_details_response = await client.get(
                    f"https://graph.microsoft.com/v1.0/servicePrincipals/{sp_id}?$select=id,appId,keyCredentials",
                    headers=headers,
                    timeout=30.0
                )

                if sp_details_response.status_code == 200:
                    sp_details = sp_details_response.json()
                    key_credentials = sp_details.get('keyCredentials', [])

                    # Find the active token signing certificate
                    saml_signing_cert = None
                    for cred in key_credentials:
                        if cred.get('usage') == 'Verify' and cred.get('type') == 'AsymmetricX509Cert':
                            saml_signing_cert = cred
                            break

                    if saml_signing_cert:
                        import base64
                        import hashlib
                        from datetime import datetime

                        certificate_thumbprint = saml_signing_cert.get('customKeyIdentifier', '')
                        if certificate_thumbprint:
                            # Convert from base64 to hex format
                            thumbprint_bytes = base64.b64decode(certificate_thumbprint)
                            certificate_thumbprint = thumbprint_bytes.hex().upper()

                        certificate_expiration = saml_signing_cert.get('endDateTime', '')
                        certificate_status = "Active"

                        # Get the actual certificate data
                        cert_key = saml_signing_cert.get('key', '')
                        if cert_key:
                            certificate_data = base64.b64encode(base64.b64decode(cert_key)).decode('utf-8')

                        print(f"✓ Found active SAML signing certificate")
                        print(f"  Thumbprint: {certificate_thumbprint}")
                        print(f"  Expiration: {certificate_expiration}")
                        print(f"  Status: {certificate_status}")
                    else:
                        print(f"⚠ No token signing certificate found in keyCredentials")
                        print(f"  Attempting to retrieve from metadata XML...")
                else:
                    print(f"⚠ Could not retrieve Service Principal details (status {sp_details_response.status_code})")

            except Exception as e:
                print(f"⚠ Error retrieving certificate from Service Principal: {e}")

            # Fallback: Fetch certificate from metadata XML if not found in keyCredentials
            if not certificate_data:
                print("Fetching SAML signing certificate from metadata XML...")
                try:
                    metadata_response = await client.get(metadata_url, headers={}, timeout=30.0)
                    if metadata_response.status_code == 200:
                        import re
                        import hashlib
                        import base64
                        metadata_xml = metadata_response.text

                        # Extract certificate from metadata XML
                        cert_match = re.search(r'<X509Certificate>(.*?)</X509Certificate>', metadata_xml, re.DOTALL)
                        if cert_match:
                            certificate_data = cert_match.group(1).strip()

                            # Calculate thumbprint from certificate
                            cert_der = base64.b64decode(certificate_data)
                            certificate_thumbprint = hashlib.sha1(cert_der).hexdigest().upper()

                            print(f"✓ Retrieved SAML signing certificate from metadata")
                            print(f"  Thumbprint: {certificate_thumbprint}")
                        else:
                            print(f"⚠ Could not extract certificate from metadata")
                    else:
                        print(f"⚠ Could not fetch metadata (status {metadata_response.status_code})")
                except Exception as e:
                    print(f"⚠ Error fetching certificate from metadata: {e}")

            # Verify configuration
            await asyncio.sleep(1)
            verify_response = await client.get(
                f"https://graph.microsoft.com/v1.0/servicePrincipals/{sp_id}",
                headers=headers,
                timeout=30.0
            )

            if verify_response.status_code == 200:
                sp_verify = verify_response.json()
                sso_mode = sp_verify.get('preferredSingleSignOnMode', 'unknown')
                print(f"✓ Verified SSO Mode: {sso_mode}")

                if sso_mode != "saml":
                    print(f"  ⚠ WARNING: SSO mode is '{sso_mode}' instead of 'saml'")
                    print(f"  Manual configuration may be required")

            print(f"✓ Generated Federation Metadata URL\n")

            # Step 8: Assign users with names starting with "mwaa" to the enterprise application
            print("Assigning MWAA users to Enterprise Application...")

            try:
                # Search for users with displayName or userPrincipalName starting with "mwaa"
                users_response = await client.get(
                    "https://graph.microsoft.com/v1.0/users?$filter=startswith(userPrincipalName,'mwaa') or startswith(displayName,'mwaa')",
                    headers=headers,
                    timeout=30.0
                )

                if users_response.status_code == 200:
                    users_data = users_response.json()
                    mwaa_users = users_data.get('value', [])

                    if mwaa_users:
                        print(f"Found {len(mwaa_users)} MWAA user(s):")
                        assigned_count = 0

                        for user in mwaa_users:
                            user_id = user['id']
                            user_name = user.get('displayName', user.get('userPrincipalName', 'Unknown'))

                            # Assign user to the enterprise application
                            assignment_payload = {
                                "principalId": user_id,
                                "resourceId": sp_id,
                                "appRoleId": "00000000-0000-0000-0000-000000000000"  # Default access role
                            }

                            assign_response = await client.post(
                                f"https://graph.microsoft.com/v1.0/servicePrincipals/{sp_id}/appRoleAssignedTo",
                                json=assignment_payload,
                                headers=headers,
                                timeout=30.0
                            )

                            if assign_response.status_code == 201:
                                print(f"  ✓ Assigned user: {user_name}")
                                assigned_count += 1
                            else:
                                print(f"  ⚠ Could not assign user {user_name} (status {assign_response.status_code})")

                        print(f"\n✓ Successfully assigned {assigned_count} out of {len(mwaa_users)} MWAA user(s)\n")
                    else:
                        print(f"No users found with names starting with 'mwaa'\n")
                else:
                    print(f"⚠ Could not search for users (status {users_response.status_code})")
                    print(f"  You'll need to assign users manually in Azure Portal\n")

            except Exception as e:
                print(f"⚠ Error assigning users: {e}")
                print(f"  You can assign users manually in Azure Portal\n")

            # Success summary
            print("=" * 80)
            print("✅ Non-Gallery SAML Application Created Successfully!")
            print("=" * 80)

            print(f"\n📋 Application Details:")
            print("-" * 80)
            print(f"Application Name:       {full_app_name}")
            print(f"Tenant ID:              {tenant_id}")
            print(f"Application ID:         {app_id}")
            print(f"Client ID:              {app_client_id}")
            print(f"Service Principal ID:   {sp_id}")

            print(f"\n🔗 SAML Configuration:")
            print("-" * 80)
            print(f"Entity ID (Identifier): {entity_id}")
            print(f"Reply URL (ACS URL):    {reply_url}")
            print(f"Sign-on URL:            {sign_on_url}")

            print(f"\n📄 Federation Metadata URL (for AWS Cognito):")
            print("-" * 80)
            print(f"{metadata_url}")

            print(f"\n🔐 Azure AD URLs (for Cognito SAML Identity Provider):")
            print("-" * 80)
            print(f"Login URL:                  {login_url}")
            print(f"Microsoft Entra Identifier: {entra_identifier}")
            print(f"Logout URL:                 {logout_url}")

            if certificate_data:
                print(f"\n🔒 SAML Certificates (Token Signing Certificate):")
                print("-" * 80)
                print(f"Status:                     {certificate_status}")
                if certificate_thumbprint:
                    print(f"Thumbprint:                 {certificate_thumbprint}")
                if certificate_expiration:
                    print(f"Expiration:                 {certificate_expiration}")
                print(f"App Federation Metadata:    {metadata_url}")
                print()
                print(f"Certificate Downloads:")
                print(f"  - Certificate (Base64):   Available in Azure Portal")
                print(f"  - Certificate (Raw):      Available in Azure Portal")
                print(f"  - Federation Metadata XML: Available at metadata URL above")
                print()
                print(f"Certificate (Base64/PEM Format):")
                print("-" * 80)
                # Format certificate for display
                cert_formatted = f"-----BEGIN CERTIFICATE-----\n"
                # Add line breaks every 64 characters
                for i in range(0, len(certificate_data), 64):
                    cert_formatted += certificate_data[i:i+64] + "\n"
                cert_formatted += "-----END CERTIFICATE-----"

                print(cert_formatted)

            print(f"\n🌐 Azure Portal URLs:")
            print("-" * 80)
            print(f"Enterprise Application:")
            print(f"  https://portal.azure.com/#view/Microsoft_AAD_IAM/ManagedAppMenuBlade/~/Overview/objectId/{sp_id}/appId/{app_client_id}")
            print(f"\nSAML Single Sign-On Configuration:")
            print(f"  https://portal.azure.com/#view/Microsoft_AAD_IAM/ManagedAppMenuBlade/~/SingleSignOn/objectId/{sp_id}/appId/{app_client_id}")

            print(f"\n📝 Next Steps:")
            print("-" * 80)
            print(f"1. Verify in Azure Portal:")
            print(f"   - Open the Enterprise Application link above")
            print(f"   - Check if it shows 'SAML-based Sign-on'")
            print(f"   - Go to Single sign-on → Review Basic SAML Configuration")
            print(f"   - Verify Entity ID, Reply URL, and Sign-on URL are set correctly")
            print(f"")
            print(f"2. Review Attributes & Claims:")
            print(f"   - Verify default attributes (email, name, etc.)")
            print(f"   - Group claim should be configured as 'SecurityGroup'")
            print(f"")
            print(f"3. Download SAML Certificates (if needed):")
            print(f"   - In SAML Certificates section")
            print(f"   - Download 'Certificate (Base64)' if needed")
            print(f"   - Download 'Federation Metadata XML' if needed")
            print(f"")
            print(f"4. Configure AWS Cognito:")
            print(f"   - Go to Cognito User Pool → Sign-in experience")
            print(f"   - Add identity provider → SAML")
            print(f"   - Provider name: AzureAD")
            print(f"   - Metadata document URL: {metadata_url}")
            print(f"   - OR manually enter Azure AD URLs:")
            print(f"     • Login URL: {login_url}")
            print(f"     • Microsoft Entra Identifier: {entra_identifier}")
            print(f"     • Logout URL: {logout_url}")
            print(f"   - Configure attribute mapping:")
            print(f"     • email → http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress")
            print(f"     • name → http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name")
            print(f"")
            print(f"5. Review User Assignments:")
            print(f"   - Go to Users and groups in Enterprise Application")
            print(f"   - Verify MWAA users were assigned correctly")
            print(f"   - Add additional users/groups if needed")
            print(f"")
            print(f"6. Test SSO:")
            print(f"   - Use the test feature in Azure Portal")
            print(f"   - Or access your ALB URL and test end-to-end")
            print(f"")
            print("=" * 80)

            return {
                "success": True,
                "tenant_id": tenant_id,
                "app_id": app_id,
                "client_id": app_client_id,
                "sp_id": sp_id,
                "app_name": full_app_name,
                "stack_name": stack_name,
                "entity_id": entity_id,
                "reply_url": reply_url,
                "sign_on_url": sign_on_url,
                "metadata_url": metadata_url,
                "login_url": login_url,
                "entra_identifier": entra_identifier,
                "logout_url": logout_url,
                "certificate_thumbprint": certificate_thumbprint,
                "certificate_expiration": certificate_expiration,
                "certificate_status": certificate_status
            }

    finally:
        await credential.close()


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create Non-Gallery SAML Enterprise Application",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python create_nongallery_saml_app.py \\
    --name "MWAA-Cognito-SAML" \\
    --entity-id "urn:amazon:cognito:sp:us-east-1_UibBymSE1" \\
    --reply-url "https://domain.auth.us-east-1.amazoncognito.com/saml2/idpresponse" \\
    --sign-on-url "https://alb.elb.amazonaws.com" \\
    --stack-name "af3-1"
        """
    )

    parser.add_argument(
        "--name",
        required=True,
        help="Display name for the Enterprise Application"
    )

    parser.add_argument(
        "--entity-id",
        required=True,
        help="SAML Entity ID (e.g., urn:amazon:cognito:sp:us-east-1_XXXXX)"
    )

    parser.add_argument(
        "--reply-url",
        required=True,
        help="SAML Reply URL / Assertion Consumer Service URL"
    )

    parser.add_argument(
        "--sign-on-url",
        required=True,
        help="SAML Sign-on URL"
    )

    parser.add_argument(
        "--stack-name",
        required=False,
        help="Stack name to append to application name for uniqueness (e.g., 'af3-1', 'dev', 'prod')"
    )
    
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Output result as JSON for script parsing"
    )

    args = parser.parse_args()

    try:
        result = await create_nongallery_saml_app(
            app_name=args.name,
            entity_id=args.entity_id,
            reply_url=args.reply_url,
            sign_on_url=args.sign_on_url,
            stack_name=args.stack_name
        )

        if result:
            # Always output JSON for script parsing
            print("\n" + "=" * 80)
            print("JSON Output (for automation):")
            print("=" * 80)
            print(json.dumps(result, indent=2))
            print("=" * 80)

            if not args.json_output:
                print("\n✅ Script completed successfully!")
            return 0
        else:
            if not args.json_output:
                print("\n❌ Script failed!")
            return 1

    except Exception as e:
        if not args.json_output:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
        else:
            # Output error as JSON
            error_result = {
                "success": False,
                "error": str(e)
            }
            print(json.dumps(error_result, indent=2))
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
