# Azure SSO Setup for MWAA (Airflow 3.x)

> **For complete documentation, see the [main README](../../README.md#azure-sso-integration).**

This directory contains Python scripts to automate the creation and configuration of Azure AD SAML Enterprise Applications for MWAA Single Sign-On integration.

## Quick Start

### Automated Setup (Recommended)

Use the `--setup-azure-sso` flag with the deployment script:

```bash
cd /path/to/mwaa-rbac-custom-roles/airflow3

# Full deployment with Azure SSO
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-alb --setup-azure-sso
```

This automatically:
- Creates Azure Enterprise Application with SAML configuration
- Generates 3-year valid SAML token signing certificate
- Retrieves Metadata URL, Login URL, and certificate details
- Updates deployment-config.json with Azure SSO URLs
- Configures Cognito SAML identity provider

### Manual Setup

If you need manual control:

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the script
python create_nongallery_saml_app.py \
  --name "MWAA-Cognito-SAML" \
  --entity-id "urn:amazon:cognito:sp:us-east-1_XXXXX" \
  --reply-url "https://your-domain.auth.us-east-1.amazoncognito.com/saml2/idpresponse" \
  --sign-on-url "https://your-alb.elb.amazonaws.com" \
  --stack-name "my-mwaa"
```

## Prerequisites

1. **Azure CLI** installed and authenticated:
   ```bash
   # macOS
   brew install azure-cli
   
   # Login
   az login
   ```

2. **Azure AD Permissions**:
   - `Application.ReadWrite.All` - Create and manage applications
   - `User.Read.All` - Read user information
   - `AppRoleAssignment.ReadWrite.All` - Assign users to applications

3. **Python 3.8+** with required packages (auto-installed by deployment script)

## Scripts

### create_nongallery_saml_app.py

Main script for creating Azure AD SAML Enterprise Applications.

**Features:**
- Creates non-gallery SAML application
- Configures SAML SSO with Entity ID, Reply URL, and Sign-on URL
- **Generates 3-year valid SAML token signing certificate**
- Sets up Security Group claims in SAML tokens
- Automatically assigns users with names starting with "mwaa"
- Provides Federation Metadata URL and Azure AD URLs
- Outputs JSON for automation

**Parameters:**
- `--name`: Base name for the application
- `--entity-id`: SAML Entity ID from Cognito (format: `urn:amazon:cognito:sp:<user-pool-id>`)
- `--reply-url`: SAML Reply URL from Cognito
- `--sign-on-url`: ALB DNS URL
- `--stack-name`: Stack identifier for uniqueness (optional)

### verify_config.py

Utility script to verify SAML configuration via Microsoft Graph API.

**Usage:**
```bash
python verify_config.py <service-principal-id>
```

## What Gets Created

1. **Azure App Registration** - Backend SAML configuration
2. **Enterprise Application** - Service Principal with SAML SSO
3. **SAML Configuration** - Entity ID, Reply URL, Sign-on URL
4. **SAML Token Signing Certificate** - 3-year valid certificate
5. **Group Claims** - Security Group membership in SAML tokens
6. **User Assignments** - Automatic assignment of MWAA users

## Certificate Generation

The script automatically generates SAML token signing certificates:
- **Validity**: 3 years
- **Method**: Azure `addTokenSigningCertificate` API
- **Output**: Thumbprint, expiration date, PEM format
- **Benefit**: No more certificate expiration errors!

## Post-Setup

### 1. Verify in Azure Portal

Open the Enterprise Application link from script output and verify:
- ✅ Single sign-on shows "SAML-based Sign-on"
- ✅ Basic SAML Configuration has Entity ID, Reply URL, Sign-on URL
- ✅ Attributes & Claims includes Group membership claims
- ✅ Users and groups shows assigned MWAA users

### 2. Test SSO

Access your ALB URL:
```
https://<alb-dns>/aws_mwaa/aws-console-sso
```

Should redirect to Azure AD login, then back to MWAA Airflow UI.

## Troubleshooting

### Authentication Errors

**Error:** "DefaultAzureCredential failed to retrieve a token"

**Solution:**
```bash
az logout
az login
az account show
```

### Permission Errors

**Error:** "Insufficient privileges to complete the operation"

**Solution:** Grant required permissions:
```bash
# Via Azure CLI
az ad app permission admin-consent --id <appId>
```

### Application Already Exists

The script automatically deletes existing apps with the same Entity ID. If it fails:
```bash
# List applications
az ad app list --query "[?identifierUris[0]=='urn:amazon:cognito:sp:us-east-1_XXXXX'].{Name:displayName, ID:appId}"

# Delete manually
az ad app delete --id <appId>
```

## Files

| File | Purpose |
|------|---------|
| `create_nongallery_saml_app.py` | Creates Azure AD SAML Enterprise Application |
| `verify_config.py` | Verifies SAML configuration via API |
| `requirements.txt` | Python dependencies (azure-identity, httpx) |
| `azure_config.template.json` | Template for storing configuration values |
| `.gitignore` | Git ignore rules |
| `README.md` | This file |

## Additional Resources

- [Main README - Azure SSO Integration](../../README.md#azure-sso-integration)
- [Azure AD SAML SSO Configuration](https://learn.microsoft.com/en-us/azure/active-directory/manage-apps/configure-saml-single-sign-on)
- [Cognito SAML Identity Providers](https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-pools-saml-idp.html)
- [Azure CLI Documentation](https://learn.microsoft.com/en-us/cli/azure/)

## Support

For detailed documentation, troubleshooting, and examples, see the [main README](../../README.md).
