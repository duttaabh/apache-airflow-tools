"""
Remote Shell Debug DAG for MWAA (S3-based Command/Response)

⚠️  IMPORTANT: MWAA uses AWS Fargate containers, NOT EC2 instances!
    - SSM Session Manager does NOT work (no instance IDs)
    - SSH access is NOT possible (no direct network access)
    - This S3-based approach is the RECOMMENDED method for Fargate

This DAG supports two modes:

  Mode 1 - Quick Command (pass "command" param when triggering):
    Runs the command immediately and prints the output. No S3 interaction needed.
    Trigger with conf: {"command": "pip list | grep celery"}

  Mode 2 - Interactive Session (no "command" param):
    Polls S3 for commands and writes results back, simulating an interactive shell.

Setup:
1. Set the Airflow Variable "s3_bucket" to your MWAA bucket name
   (Admin > Variables in the UI, or: airflow variables set s3_bucket YOUR_BUCKET)
2. Trigger the DAG — optionally with conf: {"command": "your shell command"}
3. For interactive mode, upload commands to:
   s3://YOUR_BUCKET/debug-shell/commands/command_TIMESTAMP.txt
4. Check results in:
   s3://YOUR_BUCKET/debug-shell/results/result_TIMESTAMP.txt

Quick Command Examples (via DAG trigger conf):
  {"command": "python3 -c 'import kombu; print(kombu.__version__)'"}
  {"command": "pip list | grep -E 'kombu|celery|airflow'"}
  {"command": "df -h"}
  {"command": "env | grep AIRFLOW | sort"}
"""

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import subprocess
import os
import sys
import socket

# ============================================================================
# CONFIGURATION
# ============================================================================
# S3_BUCKET is read from the Airflow Variable "s3_bucket" at task runtime.
# Set it via the Airflow UI (Admin > Variables) or CLI:
#   airflow variables set s3_bucket your-mwaa-s3-bucket-name
# ============================================================================
S3_COMMAND_PREFIX = "debug-shell/commands/"
S3_RESULT_PREFIX = "debug-shell/results/"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
}


def _execute_command(command, timeout=60):
    """Execute a shell command and return a formatted result dict."""
    print(f"Command: {command}")
    print("Executing...")

    start_time = time.time()
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd='/usr/local/airflow',
        )
        execution_time = time.time() - start_time

        result_text = (
            f"Command: {command}\n"
            f"Hostname: {socket.gethostname()}\n"
            f"Timestamp: {datetime.now().isoformat()}\n"
            f"Execution Time: {execution_time:.2f} seconds\n"
            f"Exit Code: {result.returncode}\n"
            f"\n=== STDOUT ===\n{result.stdout}\n"
            f"\n=== STDERR ===\n{result.stderr}\n"
            f"\n=== END ===\n"
        )
        return {
            'text': result_text,
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'execution_time': execution_time,
            'error': None,
        }

    except subprocess.TimeoutExpired:
        error_msg = f"Command timed out after {timeout} seconds"
        print(f"❌ {error_msg}")
        return {'text': f"ERROR: {error_msg}", 'error': error_msg}

    except Exception as e:
        error_msg = f"Error executing command: {e}"
        print(f"❌ {error_msg}")
        return {'text': f"ERROR: {error_msg}", 'error': error_msg}


def remote_shell_session(**context):
    """
    Two modes:
      - Quick command: pass {"command": "..."} in the DAG trigger conf.
        Runs the command, prints output, and exits.
      - Interactive session: no "command" param — polls S3 for commands.
    """
    import boto3
    from botocore.exceptions import ClientError

    # ----------------------------------------------------------------
    # Check for quick-command mode
    # ----------------------------------------------------------------
    dag_conf = context.get('dag_run').conf or {}
    quick_command = dag_conf.get('command')

    print("=" * 80)
    print("🖥️  REMOTE SHELL SESSION STARTED")
    print("=" * 80)
    print(f"\nHostname: {socket.gethostname()}")
    print(f"Python: {sys.version}")
    print(f"Working Directory: {os.getcwd()}")
    print(f"User: {os.getenv('USER', 'unknown')}")

    if quick_command:
        print(f"\n⚡ QUICK COMMAND MODE")
        print(f"=" * 80)
        result = _execute_command(quick_command)
        print(result['text'])
        return {
            'mode': 'quick_command',
            'hostname': socket.gethostname(),
            'command': quick_command,
            'exit_code': result.get('returncode'),
            'error': result.get('error'),
        }

    # ----------------------------------------------------------------
    # Interactive S3 polling mode
    # ----------------------------------------------------------------
    S3_BUCKET = Variable.get("s3_bucket")

    print(f"\nS3 Bucket: {S3_BUCKET}")
    print(f"Command Prefix: {S3_COMMAND_PREFIX}")
    print(f"Result Prefix: {S3_RESULT_PREFIX}")
    print("=" * 80)

    # Initialize S3 client
    try:
        s3 = boto3.client('s3')
        print("\n✅ S3 client initialized")
    except Exception as e:
        print(f"\n❌ Failed to initialize S3 client: {e}")
        return {'error': 'S3 initialization failed'}

    # Test S3 access
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        print(f"✅ S3 bucket '{S3_BUCKET}' is accessible")
    except ClientError as e:
        print(f"❌ Cannot access S3 bucket '{S3_BUCKET}': {e}")
        print("Please set the Airflow Variable 's3_bucket' to a valid bucket")
        return {'error': 'S3 bucket not accessible'}

    print("\n" + "=" * 80)
    print("📡 REMOTE SHELL READY - S3 COMMAND/RESPONSE MODE")
    print("=" * 80)
    print(f"\n🔧 CONFIGURATION:")
    print(f"   S3 Bucket: {S3_BUCKET}")
    print(f"   Command Folder: s3://{S3_BUCKET}/{S3_COMMAND_PREFIX}")
    print(f"   Result Folder: s3://{S3_BUCKET}/{S3_RESULT_PREFIX}")
    print(f"   Hostname: {socket.gethostname()}")

    print("\n" + "=" * 80)
    print("📝 HOW TO USE - COPY/PASTE THESE COMMANDS")
    print("=" * 80)

    print("\n1️⃣  SEND A COMMAND:")
    print(f'\n   # Example: Check kombu version')
    print(f'   echo "python3 -c \'import kombu; print(kombu.__version__)\'" > /tmp/cmd.txt')
    print(f'   aws s3 cp /tmp/cmd.txt s3://{S3_BUCKET}/{S3_COMMAND_PREFIX}command_$(date +%s).txt')

    print("\n2️⃣  WAIT ~15 SECONDS, THEN GET RESULT:")
    print(f'\n   # List results')
    print(f'   aws s3 ls s3://{S3_BUCKET}/{S3_RESULT_PREFIX}')
    print(f'   ')
    print(f'   # View latest result')
    print(f'   aws s3 cp s3://{S3_BUCKET}/{S3_RESULT_PREFIX}$(aws s3 ls s3://{S3_BUCKET}/{S3_RESULT_PREFIX} | tail -1 | awk \'{{print $4}}\') -')

    print("\n" + "=" * 80)
    print("💡 READY-TO-USE COMMANDS")
    print("=" * 80)

    print("\n# Check kombu version:")
    print(f'echo "python3 -c \'import kombu; print(kombu.__version__)\'" > /tmp/cmd.txt && aws s3 cp /tmp/cmd.txt s3://{S3_BUCKET}/{S3_COMMAND_PREFIX}command_$(date +%s).txt')

    print("\n# List installed packages:")
    print(f'echo "pip list | grep -E \'kombu|celery|airflow\'" > /tmp/cmd.txt && aws s3 cp /tmp/cmd.txt s3://{S3_BUCKET}/{S3_COMMAND_PREFIX}command_$(date +%s).txt')

    print("\n# Check environment variables:")
    print(f'echo "env | grep AIRFLOW | sort" > /tmp/cmd.txt && aws s3 cp /tmp/cmd.txt s3://{S3_BUCKET}/{S3_COMMAND_PREFIX}command_$(date +%s).txt')

    print("\n# List DAG folder:")
    print(f'echo "ls -la /usr/local/airflow/dags/" > /tmp/cmd.txt && aws s3 cp /tmp/cmd.txt s3://{S3_BUCKET}/{S3_COMMAND_PREFIX}command_$(date +%s).txt')

    print("\n# Check disk space:")
    print(f'echo "df -h" > /tmp/cmd.txt && aws s3 cp /tmp/cmd.txt s3://{S3_BUCKET}/{S3_COMMAND_PREFIX}command_$(date +%s).txt')

    print("\n# Test Celery import:")
    print(f'echo "python3 -c \'from celery import Celery; print(\\\"Celery OK\\\")\'" > /tmp/cmd.txt && aws s3 cp /tmp/cmd.txt s3://{S3_BUCKET}/{S3_COMMAND_PREFIX}command_$(date +%s).txt')

    print("\n" + "=" * 80)
    print("⏱️  SESSION ACTIVE FOR 30 MINUTES")
    print("=" * 80)
    print("\nSend as many commands as you want during this time.")
    print("Each command will be executed and results saved to S3.")
    print("=" * 80)

    # Track processed commands
    processed_commands = set()
    command_count = 0

    # Run for 30 minutes
    duration_minutes = 30
    end_time = time.time() + (duration_minutes * 60)
    poll_interval = 10  # Check for new commands every 10 seconds

    while time.time() < end_time:
        remaining = int((end_time - time.time()) / 60)

        try:
            # List command files in S3
            response = s3.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=S3_COMMAND_PREFIX,
            )

            if 'Contents' in response:
                for obj in response['Contents']:
                    command_key = obj['Key']

                    if command_key in processed_commands or command_key == S3_COMMAND_PREFIX:
                        continue

                    print(f"\n{'=' * 80}")
                    print(f"📥 NEW COMMAND RECEIVED: {command_key}")
                    print(f"{'=' * 80}")

                    try:
                        cmd_response = s3.get_object(Bucket=S3_BUCKET, Key=command_key)
                        command = cmd_response['Body'].read().decode('utf-8').strip()

                        result = _execute_command(command)

                        result_key = command_key.replace(
                            S3_COMMAND_PREFIX, S3_RESULT_PREFIX
                        ).replace('command_', 'result_')

                        s3.put_object(
                            Bucket=S3_BUCKET,
                            Key=result_key,
                            Body=result['text'].encode('utf-8'),
                        )

                        print(f"✅ Result uploaded to: s3://{S3_BUCKET}/{result_key}")
                        if result.get('stdout'):
                            print(f"Output (first 500 chars):\n{result['stdout'][:500]}")

                        processed_commands.add(command_key)
                        command_count += 1

                        s3.delete_object(Bucket=S3_BUCKET, Key=command_key)
                        print("🗑️  Command file deleted")

                    except Exception as e:
                        error_msg = f"Error processing command file: {e}"
                        print(f"❌ {error_msg}")
                        processed_commands.add(command_key)

            # Heartbeat
            if int(time.time()) % 60 == 0:
                print(
                    f"\n[{datetime.now().strftime('%H:%M:%S')}] "
                    f"💓 Heartbeat - {remaining} min remaining, "
                    f"{command_count} commands processed"
                )

        except Exception as e:
            print(f"\n❌ Error polling S3: {e}")

        time.sleep(poll_interval)

    print("\n" + "=" * 80)
    print("✅ REMOTE SHELL SESSION COMPLETED")
    print("=" * 80)
    print(f"Total commands processed: {command_count}")

    return {
        'mode': 'interactive',
        'hostname': socket.gethostname(),
        'commands_processed': command_count,
        'duration_minutes': duration_minutes,
    }


def setup_s3_folders(**context):
    """Create S3 folders for command/result exchange.
    Skipped automatically in quick-command mode.
    """
    import boto3

    dag_conf = context.get('dag_run').conf or {}
    if dag_conf.get('command'):
        print("⚡ Quick command mode — skipping S3 folder setup")
        return {'status': 'skipped', 'reason': 'quick_command_mode'}

    S3_BUCKET = Variable.get("s3_bucket")
    print(f"Setting up S3 folders in bucket: {S3_BUCKET}")

    try:
        s3 = boto3.client('s3')

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"{S3_COMMAND_PREFIX}.placeholder",
            Body=b"Command folder",
        )
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"{S3_RESULT_PREFIX}.placeholder",
            Body=b"Result folder",
        )

        print(f"✅ S3 folders created:")
        print(f"   s3://{S3_BUCKET}/{S3_COMMAND_PREFIX}")
        print(f"   s3://{S3_BUCKET}/{S3_RESULT_PREFIX}")
        return {'status': 'success'}

    except Exception as e:
        print(f"❌ Error setting up S3 folders: {e}")
        return {'status': 'error', 'message': str(e)}


# Create the DAG
with DAG(
    'debug_remote_shell',
    default_args=default_args,
    description='Remote shell via S3 polling or quick command via trigger conf',
    schedule=None,
    start_date=datetime(2026, 1, 19),
    catchup=False,
    tags=['debug', 'remote-shell', 'interactive'],
    params={'command': ''},
) as dag:

    setup = PythonOperator(
        task_id='setup_s3_folders',
        python_callable=setup_s3_folders,
        provide_context=True,
    )

    shell = PythonOperator(
        task_id='remote_shell_session',
        python_callable=remote_shell_session,
        provide_context=True,
        execution_timeout=timedelta(minutes=35),
    )

    setup >> shell
