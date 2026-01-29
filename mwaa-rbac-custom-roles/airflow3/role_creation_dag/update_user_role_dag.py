"""
DAG to update Airflow user roles dynamically by directly modifying the metadata database.

This DAG accepts username and role as input parameters via DAG run configuration
and updates the user's role accordingly.

IMPORTANT: This DAG removes ALL existing roles from the user and assigns ONLY the 
specified custom role. This ensures users have exactly one role with no conflicts.

Supports usernames with special characters including forward slashes (/)
which are common in MWAA IAM roles like 'assumed-role/RoleName'.

Note: MWAA Airflow 3.x restricts CLI and ORM access, so this DAG directly updates
the PostgreSQL metadata database using psycopg2 and MWAA environment variables.

Database Tables Used:
- ab_user: User information
- ab_role: Role definitions
- ab_user_role: User-to-role mappings (ALL entries for the user are deleted, then one is added)

Usage via REST API (from Lambda):
    mwaa.invoke_rest_api(
        Name='mwaa-environment',
        Path='/dags/update_user_role/dagRuns',
        Method='POST',
        Body={
            'conf': {
                'username': 'assumed-role/MyRole',
                'role': 'MWAARestrictedTest'
            },
            'logical_date': datetime.now(timezone.utc).isoformat()
        }
    )

Usage via Airflow UI:
    1. Click on the DAG name "update_user_role"
    2. Click "Trigger DAG" button (top right)
    3. Fill in the form fields or use JSON:
       {"username": "john.doe", "role": "MWAARestrictedTest"}
    4. Click "Trigger"
"""

import os

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.models.param import Param

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def update_user_role_db(**context):
    """
    Update user role by directly modifying the Airflow metadata database.
    Creates user if they don't exist, then sets their role.
    Properly handles usernames with special characters.
    
    Note: MWAA Airflow 3.x restricts CLI access, so we directly update the database
    using the MWAA-specific environment variables.
    """
    import json
    import psycopg2
    from psycopg2.extras import RealDictCursor
    
    # Get inputs directly from dag_run conf or params
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    params = context.get('params', {})
    
    # Get username from conf or params
    username = conf.get('username') or params.get('username')
    new_role = conf.get('role') or params.get('role')
    
    # Basic validation
    if not username or username == '':
        raise AirflowException("Missing required parameter: 'username'")
    
    if not new_role or new_role == '':
        raise AirflowException("Missing required parameter: 'role'")
    
    print(f"Processing user: '{username}' with role: '{new_role}'")
    
    # Get database connection details from MWAA environment variables
    mwaa_db_credentials = os.environ.get("DB_SECRETS")
    mwaa_db_host = os.environ.get("POSTGRES_HOST")
    mwaa_db_port = os.environ.get("POSTGRES_PORT", "5432")
    mwaa_db_name = os.environ.get("POSTGRES_DB", "AirflowMetadata")
    
    if not all([mwaa_db_credentials, mwaa_db_host]):
        raise AirflowException("MWAA database environment variables not available")
    
    # Parse MWAA credentials JSON
    try:
        credentials = json.loads(mwaa_db_credentials)
        db_username = credentials["username"]
        db_password = credentials["password"]
    except (json.JSONDecodeError, KeyError) as e:
        raise AirflowException(f"Failed to parse DB_SECRETS: {e}")
    
    print(f"Connecting to database:")
    print(f"  Host: {mwaa_db_host}")
    print(f"  Port: {mwaa_db_port}")
    print(f"  Database: {mwaa_db_name}")
    print(f"  Username: {db_username}")
    
    conn = None
    cursor = None
    
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=mwaa_db_host,
            port=mwaa_db_port,
            database=mwaa_db_name,
            user=db_username,
            password=db_password,
            connect_timeout=10
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("✓ Connected to database")
        
        # Step 1: Check if user exists
        print(f"  Checking if user exists...")
        cursor.execute("SELECT id, username, first_name, last_name, email, active FROM ab_user WHERE username = %s", (username,))
        user = cursor.fetchone()
        
        user_id = None
        
        if not user:
            print(f"  User does not exist, creating user...")
            
            # Insert new user
            cursor.execute(
                """
                INSERT INTO ab_user (username, first_name, last_name, email, active, created_on, changed_on)
                VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING id
                """,
                (
                    username,
                    username,
                    username,
                    f'{username.replace("/", "_")}@example.com',
                    True
                )
            )
            user_id = cursor.fetchone()['id']
            conn.commit()
            
            print(f"  ✓ Created user with ID: {user_id}")
        else:
            user_id = user['id']
            print(f"  ✓ User exists with ID: {user_id}")
        
        # Step 2: Get the role ID for the target role
        print(f"  Looking up role: {new_role}")
        cursor.execute("SELECT id, name FROM ab_role WHERE name = %s", (new_role,))
        role = cursor.fetchone()
        
        if not role:
            raise AirflowException(f"Role '{new_role}' does not exist in the database")
        
        role_id = role['id']
        print(f"  ✓ Found role '{new_role}' with ID: {role_id}")
        
        # Step 3: Get current user roles
        cursor.execute(
            """
            SELECT r.id, r.name 
            FROM ab_user_role ur
            JOIN ab_role r ON ur.role_id = r.id
            WHERE ur.user_id = %s
            """,
            (user_id,)
        )
        current_roles = cursor.fetchall()
        current_role_names = [r['name'] for r in current_roles]
        
        print(f"  Current roles: {current_role_names}")
        
        # Step 4: Check if user already has ONLY the target role
        if len(current_roles) == 1 and current_roles[0]['id'] == role_id:
            print(f"  User already has ONLY the '{new_role}' role, no changes needed")
            print(f"✓ User '{username}' already configured correctly")
            return
        
        # Step 5: ALWAYS remove ALL existing roles (including any standard roles)
        # This ensures the user has ONLY the custom role being passed
        print(f"  Removing ALL existing roles to ensure user has ONLY '{new_role}'...")
        cursor.execute("DELETE FROM ab_user_role WHERE user_id = %s", (user_id,))
        
        if current_roles:
            print(f"  ✓ Removed {len(current_roles)} role(s): {current_role_names}")
        else:
            print(f"  ✓ No existing roles to remove")
        
        # Step 6: Add ONLY the new custom role
        print(f"  Adding ONLY the custom role: {new_role}")
        cursor.execute(
            "INSERT INTO ab_user_role (user_id, role_id) VALUES (%s, %s)",
            (user_id, role_id)
        )
        
        # Commit the transaction
        conn.commit()
        
        print(f"  ✓ Added role: {new_role}")
        print(f"✓ Successfully updated '{username}' to role '{new_role}'")
        print(f"  User now has ONLY the '{new_role}' role")
        
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        error_msg = f"Database error: {e}"
        print(f"Error: {error_msg}")
        raise AirflowException(error_msg)
    
    except Exception as e:
        if conn:
            conn.rollback()
        import traceback
        error_msg = str(e)
        stack_trace = traceback.format_exc()
        print(f"Error details: {error_msg}")
        print(f"Stack trace:\n{stack_trace}")
        raise AirflowException(f"Failed to update user role: {error_msg}")
    
    finally:
        # Clean up database connections
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("✓ Database connection closed")


# Define the DAG
with DAG(
    dag_id='update_user_role',
    default_args=default_args,
    description='Update Airflow user role with dynamic input parameters (supports usernames with /)',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['admin', 'user-management'],
    params={
        'username': Param(
            '',
            type='string',
            description='Username to update (supports special characters like /). Example: john.doe or assumed-role/MyRole',
        ),
        'role': Param(
            '',
            type='string',
            description='New role to assign (removes all other roles). Valid roles: Admin, Op, User, Viewer, Public, Restricted',
        ),
    },
    render_template_as_native_obj=True,
) as dag:

    # Update user role using direct database access
    update_role_db_task = PythonOperator(
        task_id='update_role_db',
        python_callable=update_user_role_db,
    )
