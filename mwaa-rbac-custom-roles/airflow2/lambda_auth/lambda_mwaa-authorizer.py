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
    mwaa_role, specific_dags = get_mwaa_role(jwt_payload)  # Get the MWAA role and specific DAGs from the group mapping
    
    # Use custom:idp-name from JWT token (contains the actual user's name from Azure AD)
    # This is shorter and more meaningful than the Cognito username
    user_name = jwt_payload.get('custom:idp-name', jwt_payload.get('email', jwt_payload.get('username', 'unknown')))
    
    # Ensure username is not too long (AWS has 64 char limit for RoleSessionName and SourceIdentity)
    if len(user_name) > 64:
        # Truncate to 64 characters
        logger.warning(f'Username too long ({len(user_name)} chars), truncating to 64: {user_name}')
        user_name = user_name[:64]
    
    logger.info('Here is role: ' + role_arn)
    logger.info('Here is mwaa_role: ' + mwaa_role)
    logger.info('Here is user_name: ' + user_name)
    if specific_dags:
        logger.info('Here are specific_dags: ' + str(specific_dags))
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
                
                # Use direct database access to create/update user role for non-standard roles
                # Standard roles (Admin, Op, User, Viewer) don't need custom role creation
                standard_roles = ['Admin', 'Op', 'User', 'Viewer']
                
                if mwaa_role not in standard_roles:
                    # Use direct database access to create custom role and assign to user
                    logger.info(f'Creating/updating custom role for user: {airflow_username} with role: {mwaa_role}')
                    
                    try:
                        # Get database connection details from Glue connection
                        # The connection was created by the create_glue_connection DAG
                        glue_conn_name = f'{Amazon_MWAA_ENV_NAME}_metadata_conn'
                        
                        logger.info(f'Retrieving database credentials from Glue connection: {glue_conn_name}')
                        
                        glue_client = boto3.client('glue', region_name=AWS_REGION)
                        
                        try:
                            glue_response = glue_client.get_connection(Name=glue_conn_name)
                            connection_properties = glue_response['Connection']['ConnectionProperties']
                            
                            # Extract database connection details
                            jdbc_url = connection_properties['JDBC_CONNECTION_URL']
                            db_username = connection_properties['USERNAME']
                            db_password = connection_properties['PASSWORD']
                            
                            # Parse JDBC URL to get host, port, and database name
                            # Format: jdbc:postgresql://host:port/database
                            jdbc_parts = jdbc_url.replace('jdbc:postgresql://', '').split('/')
                            host_port = jdbc_parts[0].split(':')
                            db_host = host_port[0]
                            db_port = int(host_port[1]) if len(host_port) > 1 else 5432
                            db_name = jdbc_parts[1] if len(jdbc_parts) > 1 else 'AirflowMetadata'
                            
                            logger.info(f'✓ Database connection details retrieved from Glue: {db_host}:{db_port}/{db_name}')
                            
                        except glue_client.exceptions.EntityNotFoundException:
                            logger.error(f'Glue connection "{glue_conn_name}" not found')
                            logger.error('Please run the create_glue_connection DAG first to create the connection')
                            redirect = logout(headers, f'Database connection not configured. Please contact your administrator to run the create_glue_connection DAG.')
                            return redirect
                        
                        # Initialize database manager with connection details
                        from mwaa_database_manager import MWAADatabaseManager
                        
                        with MWAADatabaseManager(
                            db_host=db_host,
                            db_port=db_port,
                            db_name=db_name,
                            db_username=db_username,
                            db_password=db_password
                        ) as db_manager:
                            
                            # Create custom role if it doesn't exist
                            # Use 'User' as the base role (copies all non-DAG permissions from User role)
                            # Pass specific_dags to restrict access to only those DAGs
                            logger.info(f'Creating custom role "{mwaa_role}" with specific DAGs: {specific_dags}')
                            
                            role_created = db_manager.create_custom_role(
                                role_name=mwaa_role,
                                source_role='User',
                                specific_dags=specific_dags  # Pass the DAG list from GROUP_TO_ROLE_MAP
                            )
                            
                            if role_created:
                                logger.info(f'✓ Custom role "{mwaa_role}" created successfully')
                            else:
                                logger.info(f'✓ Custom role "{mwaa_role}" already exists')
                            
                            # Update user's role
                            user_updated = db_manager.update_user_role(
                                username=airflow_username,
                                role_name=mwaa_role
                            )
                            
                            if user_updated:
                                logger.info(f'✓ User "{airflow_username}" assigned to role "{mwaa_role}"')
                            else:
                                logger.error(f'✗ Failed to assign user "{airflow_username}" to role "{mwaa_role}"')
                                redirect = logout(headers, 'Failed to configure user access. Please try again or contact your administrator.')
                                return redirect
                        
                    except Exception as e:
                        logger.error(f'Error creating/updating user role: {e}')
                        import traceback
                        logger.error(f'Traceback: {traceback.format_exc()}')
                        redirect = logout(headers, 'Failed to configure user access. Please try again or contact your administrator.')
                        return redirect
                else:
                    logger.info(f'User has standard role "{mwaa_role}", no custom role creation needed')
                
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
    Returns the MWAA role and specific DAGs based on the
    'custom:idp-groups' contained in the JWT token
    
    Returns:
        tuple: (mwaa_role, specific_dags) where specific_dags is a list or None
    """
    mwaa_role = 'Public'  # Default role if no mapping found
    specific_dags = None
    
    logger.info(f'JWT payload: {jwt_payload}')
    if 'custom:idp-groups' in jwt_payload:
        user_groups = parse_groups(jwt_payload['custom:idp-groups'])
        for mapping in GROUP_TO_ROLE_MAP:
            if mapping['idp-group'] in user_groups:
                mwaa_role = mapping.get('mwaa-role', 'Public')
                specific_dags = mapping.get('specific_dags', None)
                logger.info(f'Found MWAA role: {mwaa_role} for group: {mapping["idp-group"]}')
                if specific_dags:
                    logger.info(f'Found specific DAGs: {specific_dags}')
                break
    return mwaa_role, specific_dags

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
