# Username API Extension Plugin

Provides alternative API endpoints that accept usernames with forward slashes via query parameters.

> **Found this tools helpful?** Give us a ⭐ on GitHub to show your support and help others discover these utilities!
> 
## Problem

The standard Airflow API uses path parameters:
```
GET /api/v1/users/{username}
```

When `username` contains `/` (e.g., `domain/username`), Flask treats it as multiple path segments, causing a 404 error.

## Solution

This plugin adds new endpoints using **query parameters** instead:
```
GET /api/v1/users-ext/?username=domain/username
```

Query parameters can contain any characters (when URL-encoded), so slashes work perfectly!

## Installation

### Local Airflow

**1. Copy Plugin File**
```bash
cp username_api_extension.py $AIRFLOW_HOME/plugins/
```
**2. Restart Airflow**

**3. Verify Installation**
```bash
airflow plugins | grep username_api_extension
```

You should see:
```
username_api_extension     |
```

### AWS MWAA

**1. Upload Plugin to S3**

Create plugins.zip
```bash
cd plugins/username_api_extension
zip ../plugins.zip username_api_extension.py
aws s3 cp ../plugins.zip s3://your-mwaa-bucket/
```

**2. Update MWAA Environment**

If using plugins.zip, update your MWAA environment configuration:
```bash
aws mwaa update-environment \
  --name your-env-name \
  --plugins-s3-path plugins.zip
```

**3. Wait for Environment Update**

MWAA will automatically restart to load the new plugin. This takes 20-30 minutes.

**4. Verify Installation**

```bash
# Test the endpoint
python3 test_api_session.py --mwaa --env-name your-env-name --region us-east-1
```

### 4. Test It

**For Session Authentication (MWAA, default Airflow):**
```bash
# Use the provided test script to test any local running airflow
cd plugins/username_api_extension
python3 test_api_session.py
```


## API Endpoints

### 1. Get User
```http
GET /api/v1/users-ext/?username={username}
```

**Example:**
```bash
curl -X GET "http://localhost:8080/api/v1/users-ext/?username=domain%2Fusername" \
  -u "admin:admin"
```

**Response:**
```json
{
  "username": "domain/username",
  "email": "user@example.com",
  "first_name": "John",
  "last_name": "Doe",
  "active": true,
  "roles": [{"name": "Admin"}]
}
```

### 2. Update User
```http
PATCH /api/v1/users-ext/?username={username}[&update_mask={fields}]
```

**Example:**
```bash
curl -X PATCH "http://localhost:8080/api/v1/users-ext/?username=domain%2Fusername" \
  -H "Content-Type: application/json" \
  -u "admin:admin" \
  -d '{
    "email": "newemail@example.com",
    "first_name": "Jane"
  }'
```

**With update_mask (only update specific fields):**
```bash
curl -X PATCH "http://localhost:8080/api/v1/users-ext/?username=domain%2Fusername&update_mask=email" \
  -H "Content-Type: application/json" \
  -u "admin:admin" \
  -d '{"email": "newemail@example.com"}'
```

### 3. Delete User
```http
DELETE /api/v1/users-ext/?username={username}
```

**Example:**
```bash
curl -X DELETE "http://localhost:8080/api/v1/users-ext/?username=domain%2Fusername" \
  -u "admin:admin"
```

**Response:** `204 No Content`

## Usage Examples

### Python (Basic Authentication)
```python
import requests
from requests.auth import HTTPBasicAuth

base_url = "http://localhost:8080"
auth = HTTPBasicAuth("admin", "admin")

# Get user with slash in username
username = "domain/username"
response = requests.get(
    f"{base_url}/api/v1/users-ext/",
    params={"username": username},  # Automatically URL-encoded
    auth=auth
)
print(response.json())

# Update user (partial update supported)
response = requests.patch(
    f"{base_url}/api/v1/users-ext/",
    params={"username": username},
    json={"email": "newemail@example.com"},
    auth=auth
)
print(response.json())

# Delete user
response = requests.delete(
    f"{base_url}/api/v1/users-ext/",
    params={"username": username},
    auth=auth
)
print(response.status_code)  # 204
```

### Python (Session Authentication)
```python
import requests

base_url = "http://localhost:8080"
username = "domain/username"

# Create session and login
session = requests.Session()
session.post(
    f"{base_url}/login/",
    data={"username": "admin", "password": "admin"}
)

# Get CSRF token for write operations
response = session.get(f"{base_url}/home")
csrf_token = session.cookies.get('csrf_token')

# Get user
response = session.get(
    f"{base_url}/api/v1/users-ext/",
    params={"username": username}
)
print(response.json())

# Update user (include CSRF token for PATCH/DELETE)
response = session.patch(
    f"{base_url}/api/v1/users-ext/",
    params={"username": username},
    json={"email": "newemail@example.com"},
    headers={"X-CSRFToken": csrf_token}
)
print(response.json())

# Delete user
response = session.delete(
    f"{base_url}/api/v1/users-ext/",
    params={"username": username},
    headers={"X-CSRFToken": csrf_token}
)
print(response.status_code)  # 204
```

### JavaScript/TypeScript
```typescript
const baseUrl = 'http://localhost:8080';
const auth = btoa('admin:admin');

// Get user
const username = 'domain/username';
const response = await fetch(
  `${baseUrl}/api/v1/users-ext/?username=${encodeURIComponent(username)}`,
  {
    headers: {
      'Authorization': `Basic ${auth}`
    }
  }
);
const user = await response.json();
console.log(user);

// Update user
await fetch(
  `${baseUrl}/api/v1/users-ext/?username=${encodeURIComponent(username)}`,
  {
    method: 'PATCH',
    headers: {
      'Authorization': `Basic ${auth}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      email: 'newemail@example.com'
    })
  }
);

// Delete user
await fetch(
  `${baseUrl}/api/v1/users-ext/?username=${encodeURIComponent(username)}`,
  {
    method: 'DELETE',
    headers: {
      'Authorization': `Basic ${auth}`
    }
  }
);
```

### cURL with URL Encoding
```bash
# Encode username
USERNAME="domain/username"
ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$USERNAME'))")

# Get user
curl -X GET "http://localhost:8080/api/v1/users-ext/?username=$ENCODED" \
  -u "admin:admin"

# Update user
curl -X PATCH "http://localhost:8080/api/v1/users-ext/?username=$ENCODED" \
  -H "Content-Type: application/json" \
  -u "admin:admin" \
  -d '{"email": "new@example.com"}'

# Delete user
curl -X DELETE "http://localhost:8080/api/v1/users-ext/?username=$ENCODED" \
  -u "admin:admin"
```

## Important Notes

### URL Encoding
**Always URL-encode usernames** with special characters:
- `/` becomes `%2F`
- `\` becomes `%5C`
- ` ` (space) becomes `%20`
- `@` becomes `%40`

Most HTTP libraries handle this automatically when using parameter dictionaries.

**Note:** When using Python's `requests` library with the `params` argument, URL encoding is automatic:
```python
# This is automatically encoded - no manual encoding needed
response = requests.get(url, params={"username": "domain/username"})
```

### Session Authentication
If your Airflow instance uses session-based authentication (default), you need to:
1. Login first to get a session cookie
2. Include CSRF token in headers for PATCH and DELETE requests:
   ```python
   headers = {"X-CSRFToken": csrf_token}
   ```

Check your `airflow.cfg`:
```ini
[api]
auth_backends = airflow.api.auth.backend.session  # Session auth
# or
auth_backends = airflow.api.auth.backend.basic_auth  # Basic auth
```

### Query Parameter Required
The `username` parameter **must** be provided in the query string:
```bash
# ✅ Correct
GET /api/v1/users-ext/?username=domain%2Fusername

# ❌ Wrong - missing username parameter
GET /api/v1/users-ext/

# ❌ Wrong - username in path
GET /api/v1/users-ext/domain/username
```

### Trailing Slash
The endpoint URL should include a trailing slash:
```bash
# ✅ Correct
/api/v1/users-ext/

# ⚠️  May work but not recommended
/api/v1/users-ext
```

## Testing

### Automated Test (Session Authentication)

The `test_api_session.py` script supports both local Airflow and AWS MWAA environments.

**Local Airflow:**
```bash
cd plugins/username_api_extension
python3 test_api_session.py
```

**AWS MWAA:**
```bash
cd plugins/username_api_extension

# Install boto3 if not already installed
pip install boto3

# Run test against MWAA environment
python3 test_api_session.py --mwaa --env-name your-mwaa-env --region us-east-1
```

**Custom Options:**
```bash
# Test with custom username
python3 test_api_session.py --username "domain/testuser"

# Test MWAA with custom username
python3 test_api_session.py --mwaa --env-name MyAirflowEnvironment --region us-east-1 --username "assumed-role/MyTest"

# See all options
python3 test_api_session.py --help
```

This script will:
1. Login (web token for MWAA, standard login for local)
2. Create a test user with `/` in username
3. Verify user appears in user list
4. Test GET, PATCH, DELETE operations
5. Verify all changes
6. Clean up test user

### Automated Test (Basic Authentication)

For environments using basic authentication:

```bash
cd $AIRFLOW_HOME/plugins
chmod +x test_api_extension.sh
./test_api_extension.sh
```

### Testing with AWS MWAA CLI

For testing on actual AWS MWAA environments using the AWS CLI:

```bash
cd plugins/username_api_extension

# Simple test (GET operations only - most reliable)
./test_mwaa_cli_simple.sh

# Full test (includes POST/PATCH/DELETE - may have CLI limitations)
MWAA_ENV_NAME="your-env-name" ./test_mwaa_cli.sh
```

**⚠️ AWS MWAA CLI Limitations:**

The `aws mwaa invoke-rest-api` command has significant limitations:
1. Cannot add custom headers (`Referer`, `Origin`, `X-CSRFToken`)
2. MWAA requires these headers for PATCH and DELETE operations
3. Result: Only GET and POST operations work via CLI

**Recommendation:** Use the Python script for full CRUD testing on MWAA:

```bash
python3 test_api_session.py --mwaa --env-name your-env-name --region us-east-1
```

This Python approach works reliably for all operations (GET, POST, PATCH, DELETE) on MWAA because it can include the required security headers.

### Manual Test
```bash
# 1. Create a test user with slash
airflow users create \
    --username "test/user" \
    --firstname "Test" \
    --lastname "User" \
    --role "Viewer" \
    --email "test@example.com" \
    --password "test123"

# 2. Get user via extended API
curl -X GET "http://localhost:8080/api/v1/users-ext/?username=test%2Fuser" \
  -u "admin:admin"

# 3. Update user
curl -X PATCH "http://localhost:8080/api/v1/users-ext/?username=test%2Fuser" \
  -H "Content-Type: application/json" \
  -u "admin:admin" \
  -d '{"email": "updated@example.com"}'

# 4. Delete user
curl -X DELETE "http://localhost:8080/api/v1/users-ext/?username=test%2Fuser" \
  -u "admin:admin"
```

## Error Responses

### Missing Username Parameter (400)
```json
{
  "detail": "Missing required query parameter: username",
  "status": 400,
  "title": "Bad Request",
  "type": "about:blank"
}
```

### User Not Found (404)
```json
{
  "detail": "The User with username `domain/username` was not found",
  "status": 404,
  "title": "User not found",
  "type": "about:blank"
}
```

### Validation Error (400)
```json
{
  "detail": "{'email': ['Not a valid email address.']}",
  "status": 400,
  "title": "Bad Request",
  "type": "about:blank"
}
```

### Unauthorized (401)
```json
{
  "detail": "Unauthorized",
  "status": 401,
  "title": "Unauthorized",
  "type": "about:blank"
}
```

## Comparison with Standard API

| Feature | Standard API | Extended API |
|---------|-------------|--------------|
| **Endpoint** | `/api/v1/users/{username}` | `/api/v1/users-ext/?username={username}` |
| **Slashes in username** | ❌ Not supported | ✅ Supported |
| **URL encoding required** | No | Yes (for special chars) |
| **Authentication** | Same | Same |
| **Permissions** | Same | Same |
| **Response format** | Same | Same |
| **In OpenAPI spec** | Yes | No |
| **In Swagger UI** | Yes | No |

## Troubleshooting

### Plugin Not Loading

**Check file exists:**
```bash
ls $AIRFLOW_HOME/plugins/username_api_extension.py
```

**Check for syntax errors:**
```bash
python -m py_compile $AIRFLOW_HOME/plugins/username_api_extension.py
```

**Check logs:**
```bash
grep "Username API Extension" $AIRFLOW_HOME/logs/webserver/*.log
```

### 404 Not Found

1. Verify plugin is loaded: `airflow plugins`
2. Check endpoint URL: `/api/v1/users-ext/` (with trailing slash)
3. Ensure username parameter is provided: `?username=...`
4. Restart webserver if recently installed

### 401 Unauthorized

1. Check authentication credentials
2. Verify user has appropriate permissions
3. Check auth manager configuration

### 400 Bad Request

1. Ensure username parameter is provided
2. Check JSON payload format for PATCH
3. Validate field names in update_mask
4. Ensure Content-Type header is set for PATCH

## Features

- **Preserves exact usernames** - No modification
- **Works with slashes** - Via query parameters
- **Same authentication** - Uses Airflow auth (basic or session)
- **Same permissions** - Requires same access
- **Partial updates** - PATCH supports updating only specified fields
- **Update mask** - Optional parameter to restrict which fields can be updated
- **Full CRUD** - GET, PATCH, DELETE operations
- **Error handling** - Proper HTTP status codes
- **Logging** - All operations logged  

## Compatibility

- **Airflow:** 2.8.0+
- **Python:** 3.8+
- **Auth Manager:** FAB (Flask-AppBuilder)
- **Database:** PostgreSQL, MySQL, SQLite

## Performance

- **Impact:** <5ms per API call
- **Same as standard API**
- **No caching differences**
- **No connection pool impact**

## Security

- ✅ Uses same authentication as standard API
- ✅ Uses same permission checks
- ✅ Validates all input data
- ✅ Prevents SQL injection (uses ORM)
- ✅ Logs all operations

## Limitations

1. **Not in OpenAPI spec** - Won't appear in Swagger UI
2. **Query parameter only** - Username must be in query string
3. **No batch operations** - One user at a time
4. **Manual URL encoding** - Clients must encode special characters
5. **No list endpoint** - Use standard `/api/v1/users` for listing

## When to Use This Plugin

✅ **Use this plugin if:**
- You have existing users with slashes
- You cannot modify existing usernames
- You need to preserve exact usernames
- You can update API clients

❌ **Don't use this plugin if:**
- You can sanitize usernames at creation
- You want to use standard API endpoints
- You need Swagger UI documentation
- You can't update API clients

## Alternative Solution

If you can control user creation, see the **User Creation Interceptor** plugin which automatically sanitizes usernames by replacing `/` with `_`.

## Files

- `username_api_extension.py` - Main plugin file
- `README.md` - This file
- `test_api_session.py` - **Recommended** test script (session auth, full CRUD)
- `test_mwaa_cli_simple.sh` - MWAA CLI test (GET operations only)
- `test_mwaa_cli.sh` - MWAA CLI test (full CRUD, has limitations)
- `test_api_extension.sh` - Test script (basic auth)
- `test_api_full.sh` - Test script (basic auth, full CRUD)
- `test_api_full.py` - Test script (basic auth, full CRUD)

## Support

For issues:

**Local Airflow:**
1. Run test suite: `python3 test_api_session.py`
2. Check logs: `$AIRFLOW_HOME/logs/webserver/`
3. Verify plugin loaded: `airflow plugins`
4. Test with simple username first: `?username=admin`

**AWS MWAA:**
1. Run test suite: `python3 test_api_session.py --mwaa --env-name your-env --region us-east-1`
2. Check CloudWatch logs for the MWAA environment
3. Verify plugin is uploaded to S3 plugins folder/zip
4. Ensure MWAA environment has restarted after plugin upload
5. Test with simple username first: `?username=admin`

## License

Apache License 2.0 - Same as Apache Airflow

---

**Quick Start:**
```bash
cp username_api_extension.py $AIRFLOW_HOME/plugins/
airflow webserver -D && airflow scheduler -D
airflow plugins | grep username_api_extension
curl "http://localhost:8080/api/v1/users-ext/?username=admin" -u "admin:admin"
```

**That's it!** You can now access users with slashes in their usernames via the extended API.
