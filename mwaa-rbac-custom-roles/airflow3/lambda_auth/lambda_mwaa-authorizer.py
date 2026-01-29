import os
import json
import base64
import logging
import requests
from jose import jwt
import botocore
import boto3
from urllib.parse import quote
Amazon_MWAA_ENV_NAME = os.environ.get('Amazon_MWAA_ENV_NAME','').strip()
AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID', '').strip()
COGNITO_CLIENT_ID = os.environ.get('COGNITO_CLIENT_ID','').strip()
COGNITO_DOMAIN = os.environ.get('COGNITO_DOMAIN').strip()
AWS_REGION = os.environ.get('AWS_REGION')
IDP_LOGIN_URI = os.environ.get('IDP_LOGIN_URI').strip()
GROUP_TO_ROLE_MAP = json.loads(os.environ.get('GROUP_TO_ROLE_MAP', '{}'))
ALB_COOKIE_NAME = os.environ.get('ALB_COOKIE_NAME','AWSELBAuthSessionCookie').strip()
LOGOUT_REDIRECT_DELAY = 10 # seconds
sts = boto3.client('sts')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    """
    Lambda handler
    """
    path = event['path']
    headers = event['multiValueHeaders']
    
    logger.info(f'Received request for path: {path}')
    
    if 'x-amzn-oidc-data' in headers:
        encoded_jwt = headers['x-amzn-oidc-data'][0]
        token_payload = decode_jwt(encoded_jwt)
    else:
        # There is no session, close
        logger.info('No OIDC data in headers, closing session')
        return close(headers)
    
    # Airflow 3.x redirects to /auth/login/ - redirect to our SSO endpoint
    if path.startswith('/auth/login'):
        logger.info('Airflow 3.x /auth/login detected, redirecting to /aws_mwaa/aws-console-sso')
        redirect = login(headers, token_payload)
    elif path == '/aws_mwaa/aws-console-sso':
        logger.info('Processing /aws_mwaa/aws-console-sso request')
        redirect = login(headers, token_payload)
    elif path == '/logout/':
        logger.info('Processing logout request')
        redirect = logout(headers, 'Logged out successfully')
    else:
        logger.info(f'Unknown path: {path}, logging out')
        redirect = logout(headers, '')
    
    logger.info(json.dumps(redirect))
    return redirect
    
def multivalue_to_singlevalue(headers):

    """
    Convert multi-value headers to single value
    """
    svheaders = {key: value[0] for (key, value) in headers.items()}
    logger.info(svheaders)
    return svheaders
    
def singlevalue_to_multivalue(headers):

    """
    Convert single value headers to multi-value headers
    """
    mvheaders = {key: [value] for (key, value) in headers.items()}
    return mvheaders
    
def login(headers, jwt_payload):

    """
    Function that returns a redirection to an appropriate
    URL that includes a web login token.
    """
    # Role to be determined using claims in JWT token
    role_arn = get_iam_role_arn(jwt_payload)
    mwaa_role = get_mwaa_role(jwt_payload)  # Get the MWAA role from the group mapping
    user_name = jwt_payload.get('username', role_arn)
    # Extract just the email address part, removing any prefix
    if '@' in user_name:
        # Split by underscore and take the last part which should be the email
        email_part = user_name.split('_')[-1]
        if '@' in email_part:
            user_name = email_part
        else:
            # Fallback: look for @ symbol and extract from there
            at_index = user_name.find('@')
            if at_index > 0:
                # Find the start of the email by looking backwards for non-alphanumeric chars
                start_index = at_index
                while start_index > 0 and (user_name[start_index-1].isalnum() or user_name[start_index-1] in '.-_'):
                    start_index -= 1
                user_name = user_name[start_index:]
    logger.info('Here is role: ' + role_arn)
    logger.info('Here is mwaa_role: ' + mwaa_role)
    logger.info('Here is user_name: ' + user_name)
    host = headers['host'][0]
    if role_arn:
        mwaa = get_mwaa_client(role_arn, user_name)
        if mwaa:
            # Obtain web login token for the configured environment
            try:
                mwaa_web_token = mwaa.create_web_login_token(Name=Amazon_MWAA_ENV_NAME)["WebToken"]
                logger.info('Obtained Amazon MWAA WEB TOKEN')
                
                # Decode the JWT token to get the actual username
                try:
                    # python-jose: disable all validation when verify_signature is False
                    decoded_token = jwt.decode(
                        mwaa_web_token, 
                        None, 
                        options={
                            "verify_signature": False,
                            "verify_aud": False,
                            "verify_exp": False
                        }
                    )
                    logger.info(f'Decoded JWT token: {decoded_token}')
                    
                    # The username is in the 'user' field
                    # Format: assumed-role/sso-mwaa-alb-MWAACustomRole-gcSM1l1QKqZ7TmGqeL25oGX
                    airflow_username = decoded_token.get('user', decoded_token.get('sub', user_name))
                    logger.info(f'Extracted username from JWT: {airflow_username}')
                    
                    # Validate the username format
                    if not airflow_username.startswith('assumed-role/'):
                        logger.warning(f'Username does not start with assumed-role/: {airflow_username}')
                        logger.warning(f'Using role_arn as fallback: {role_arn}')
                        airflow_username = role_arn
                        
                except Exception as e:
                    logger.error(f'Could not decode JWT token: {e}. Using role ARN as username.')
                    airflow_username = role_arn
                
                # Trigger the update_user_role DAG only for non-standard roles
                # Standard roles (Admin, Op, User, Viewer) don't need DAG updates
                standard_roles = ['Admin', 'Op', 'User', 'Viewer']
                
                if mwaa_role not in standard_roles:
                    # Trigger DAG and wait for completion
                    # Note: Airflow 3.x doesn't have /users endpoint, so we can't check existing roles
                    # The DAG will handle creating/updating the user as needed
                    logger.info(f'Triggering update_user_role DAG for user: {airflow_username} with role: {mwaa_role}')
                    dag_success = trigger_and_wait_for_dag(
                        mwaa=mwaa,
                        username=airflow_username,
                        role=mwaa_role
                    )
                    
                    if not dag_success:
                        logger.error('DAG did not complete successfully')
                        redirect = logout(headers, 'Failed to configure user access. Please try again or contact your administrator.')
                        return redirect
                    
                    logger.info('✓ DAG completed successfully, user has been created/updated with correct role')
                else:
                    logger.info(f'User has standard role "{mwaa_role}", skipping DAG trigger')
                
                # Proceed with login
                logger.info('Redirecting to MWAA UI with web token')
                redirect = {
                'statusCode': 302,
                'statusDescription': '302 Found',
                'multiValueHeaders': {
                'Location':
                [f'https://{host}/aws_mwaa/aws-console-sso?token=true#{mwaa_web_token}']
                }
                }
            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'AccessDeniedException':
                    print("here is the role " +role_arn)
                    print("here is the user_name " +user_name)
                    redirect = logout(headers,  f'The role {role_arn} assigned to {user_name} does not have access to the environment {Amazon_MWAA_ENV_NAME}.')
                    
                elif error.response['Error']['Code'] == 'ResourceNotFoundException':
                    redirect = logout(headers, f'Environment {Amazon_MWAA_ENV_NAME} was not found.')
                else:
                    redirect = logout(headers, error)
        else:
            redirect = logout(headers, 'There was an error while logging in, please contact your administrator.')
    else:
        redirect = logout(headers, 'There is no valid role associated with your user.')
    return redirect

def logout(headers, message):
    """
    Logs out from Airflow and expires the ALB cookies.
    If a message is present, it displays it for a few
    seconds and redirects to Cognito logout.
    """
    logger.info('LOGGING OUT')
    logger.info(headers)
    host = headers['host'][0]
    redirect_uri = quote(f'https://{host}/logout/close',safe="")
    cognito_logout_uri = \
    f'https://{COGNITO_DOMAIN}.auth.{AWS_REGION}.amazoncognito.com/logout?client_id=' + \
    f'{COGNITO_CLIENT_ID}&response_type=code&logout_uri={redirect_uri}&scope=openid'
    # headers = headers_to_forward
    headers['Location'] = [cognito_logout_uri]
    expire_alb_cookies(headers)
    if message:
        logger.info('inside message')
        body = error_redirection_body(message, cognito_logout_uri)
        logger.info('back inside message')
        headers['Content-Type'] = ['text/html']
        redirect = {
        'statusCode': 200,
        'multiValueHeaders': headers,
        'body': body,
        'isBase64Encoded': False
        }
    else:
        redirect = {
        'statusCode': 302,
        'statusDescription': '302 Found',
        'multiValueHeaders': headers
        }
    return redirect

def get_mwaa_client(role_arn, user_name):
    logger.info('inside get mwaa client function')

    """
    Returns an Amazon MWAA client under the given IAM
    role
    """
    mwaa = None
    try:
        logger.info(f'Assuming role "{role_arn}" with source identity "{user_name}"...')
        logger.info('Here is role: ' + role_arn)
        logger.info('Here is user_name: ' + user_name)
        credentials = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName=user_name,
        DurationSeconds=900, # This is the minimum allowed
        SourceIdentity=user_name)['Credentials']
        access_key = credentials['AccessKeyId']
        secret_key = credentials['SecretAccessKey']
        session_token = credentials['SessionToken']
        # create service client using the assumed role credentials, e.g. S3
        mwaa = boto3.client('mwaa',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token
        )
    except botocore.exceptions.ClientError as error:
        logger.error(f'Error while assuming role {role_arn}.{error}')
    except Exception as error:
        logger.error(f'Unknown error while assuming role {role_arn}. {error}')
    return mwaa


def trigger_and_wait_for_dag(mwaa, username, role, max_wait_seconds=60):
    """
    Triggers the update_user_role DAG using MWAA REST API and waits for completion.
    
    Args:
        mwaa: MWAA client
        username: Username to update
        role: Role to assign
        max_wait_seconds: Maximum time to wait for DAG completion (default: 60 seconds)
    
    Returns:
        bool: True if DAG completed successfully, False otherwise
    """
    import time
    
    try:
        # Trigger the DAG using REST API
        logger.info(f'Triggering DAG via REST API for user: {username} with role: {role}')
        
        # Prepare the request body for Airflow 3.x
        from datetime import datetime, timezone
        request_body = {
            'conf': {
                'username': username,
                'role': role
            },
            'logical_date': datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f'Request body: {request_body}')
        
        trigger_response = mwaa.invoke_rest_api(
            Name=Amazon_MWAA_ENV_NAME,
            Path='/dags/update_user_role/dagRuns',
            Method='POST',
            Body=request_body
        )
        
        logger.info(f'Trigger response status: {trigger_response.get("RestApiStatusCode")}')
        
        # Check if trigger was successful
        if trigger_response.get('RestApiStatusCode') not in [200, 201]:
            logger.error(f'Failed to trigger DAG. Status: {trigger_response.get("RestApiStatusCode")}')
            logger.error(f'Response: {trigger_response.get("RestApiResponse")}')
            return False
        
        # Extract dag_run_id from response
        rest_api_response = trigger_response.get('RestApiResponse', {})
        dag_run_id = rest_api_response.get('dag_run_id')
        
        if not dag_run_id:
            logger.error(f'Could not extract dag_run_id from response: {rest_api_response}')
            return False
        
        logger.info(f'✓ DAG triggered successfully with dag_run_id: {dag_run_id}')
        
        # Poll for DAG completion
        start_time = time.time()
        check_interval = 2  # Check every 2 seconds
        
        while time.time() - start_time < max_wait_seconds:
            elapsed = int(time.time() - start_time)
            logger.info(f'Checking DAG status... (elapsed: {elapsed}s)')
            
            # Get DAG run status
            status_response = mwaa.invoke_rest_api(
                Name=Amazon_MWAA_ENV_NAME,
                Path=f'/dags/update_user_role/dagRuns/{dag_run_id}',
                Method='GET'
            )
            
            if status_response.get('RestApiStatusCode') != 200:
                logger.warning(f'Failed to get DAG status. Status: {status_response.get("RestApiStatusCode")}')
                time.sleep(check_interval)
                continue
            
            status_data = status_response.get('RestApiResponse', {})
            state = status_data.get('state')
            
            logger.info(f'DAG run state: {state}')
            
            if state == 'success':
                logger.info(f'✓ DAG run {dag_run_id} completed successfully')
                return True
            elif state in ['failed', 'upstream_failed']:
                logger.error(f'✗ DAG run {dag_run_id} failed with state: {state}')
                return False
            elif state in ['running', 'queued']:
                logger.info(f'DAG run {dag_run_id} is still {state}...')
                time.sleep(check_interval)
            else:
                logger.warning(f'Unknown DAG state: {state}')
                time.sleep(check_interval)
        
        # Timeout reached
        logger.error(f'✗ Timeout waiting for DAG to complete after {max_wait_seconds} seconds')
        return False
            
    except Exception as e:
        logger.error(f'Error triggering or waiting for DAG: {e}')
        logger.error(f'Error type: {type(e).__name__}')
        logger.error(f'Error args: {e.args}')
        if hasattr(e, 'response'):
            logger.error(f'Error response: {e.response}')
        import traceback
        logger.error(f'Traceback: {traceback.format_exc()}')
        return False
    
def get_iam_role_arn(jwt_payload):

    """
    Returns the name of an IAM role based on the
    'custom:idp-groups' contained in the JWT token
    """
    # This list contains the mappings between IdP groups and their corresponding IAM role.
    # The list is sorted by precedence, so, if a user belongs to more than one group, it's given
    # mapped to a role that contains more permissions
    role_arn = ''
    logger.info(f'JWT payload: {jwt_payload}')
    if 'custom:idp-groups' in jwt_payload:
        user_groups = parse_groups(jwt_payload['custom:idp-groups'])
        for mapping in GROUP_TO_ROLE_MAP:
            if mapping['idp-group'] in user_groups:
                role_name = mapping['iam-role']
                role_arn = f'arn:aws:iam::{AWS_ACCOUNT_ID}:role/{role_name}'
                logger.info('here is IAM role ' + role_arn)
            
                break
    return role_arn

def get_mwaa_role(jwt_payload):
    """
    Returns the MWAA role based on the
    'custom:idp-groups' contained in the JWT token
    """
    mwaa_role = 'Public'  # Default role if no mapping found
    logger.info(f'JWT payload: {jwt_payload}')
    if 'custom:idp-groups' in jwt_payload:
        user_groups = parse_groups(jwt_payload['custom:idp-groups'])
        for mapping in GROUP_TO_ROLE_MAP:
            if mapping['idp-group'] in user_groups:
                mwaa_role = mapping.get('mwaa-role', 'Public')
                logger.info(f'Found MWAA role: {mwaa_role} for group: {mapping["idp-group"]}')
                break
    return mwaa_role

def parse_groups(groups):

    """
    Converts the groups SAML claim content to a list of
    strings
    """
    # The groups SAML claim comes in a string
    # When there is more than one group id, the string starts and ends with square brackets
    # There might also be spaces between the group ids
    groups = groups.replace('[', '').replace(']','').replace(' ', '')
    return groups.split(',')

def decode_jwt(encoded_jwt):
    """
    Decodes a JSON Web Token issued by the ALB after
    successful authentication
    against an OIDC IdP (e.g.: Cognito).
    https://docs.aws.amazon.com/elasticloadbalancing/latest/appli
    cation/listener-authenticate-users.html
    """
    # Step 1: Get the key id from JWT headers (the kid field)
    jwt_headers = encoded_jwt.split('.')[0]
    decoded_jwt_headers = base64.b64decode(jwt_headers)
    decoded_jwt_headers = decoded_jwt_headers.decode("utf-8")
    decoded_json = json.loads(decoded_jwt_headers)
    kid = decoded_json['kid']
    # Step 2: Get the public key from regional endpoint
    url = f'https://public-keys.auth.elb.{AWS_REGION}.amazonaws.com/{kid}'
    req = requests.get(url)
    pub_key = req.text
    # Step 3: Get the payload using python-jose
    payload = jwt.decode(encoded_jwt, pub_key, algorithms=[decoded_json['alg']])
    return payload

def expire_alb_cookies(headers):

    """
    Sets ALB session cookies to expire
    """
    alb_cookies = [f'{ALB_COOKIE_NAME}-1=del;Max-Age=-1;Path=/;',f'{ALB_COOKIE_NAME}-0=del;Max-Age=-1;Path=/;']
    if 'Set-Cookie' in headers:
        headers['Set-Cookie'] += alb_cookies
    else:
        headers['Set-Cookie'] = alb_cookies

def error_redirection_body(message, logout_uri):

    """
    Returns an HTML string that displays an error message
    and redirects the browser to the logout_uri
    """
    body = f'<html><body><h3>{message}</h3><br><br>Closing session in ' + \
    f'<span id="countdown">{LOGOUT_REDIRECT_DELAY}</span> seconds' + \
    '</body></html><script type="text/javascript">' + \
    f'var seconds = {LOGOUT_REDIRECT_DELAY};' + \
    'function countdown() {' + \
    ' seconds -= 1;' + \
    ' if (seconds < 0) {' + \
    f' window.location = "{logout_uri}";' + \
    ' } else {' + \
    ' document.getElementById("countdown").innerHTML = seconds;' + \
    ' window.setTimeout("countdown()", 1000);' + \
    ' }' + \
    '}' + \
    'countdown();' + \
    '</script>'
    return body

def close(headers):

    """
    Requests user to close the current tab
    """
    body = '<html><body><h3>You can now close this tab.</h3></body></html>'
    headers['Content-Type'] = ['text/html']
    return {
    'statusCode': 200,
    'multiValueHeaders': headers,
    'body': body,
    'isBase64Encoded': False
    }
