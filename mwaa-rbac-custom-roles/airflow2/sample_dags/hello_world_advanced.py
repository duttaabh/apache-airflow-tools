"""
Advanced Hello World DAG for Apache Airflow 2.10.3

This DAG demonstrates multiple operators and parallel task execution
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


def greet_user(**context):
    """Function to greet with execution date"""
    execution_date = context['execution_date']
    print(f"Hello! DAG executed on: {execution_date}")
    return f"Greeting sent at {execution_date}"


def process_data(**context):
    """Simulate data processing"""
    task_instance = context['task_instance']
    data = {'records_processed': 100, 'status': 'success'}
    print(f"Processing data: {data}")
    # Push data to XCom for downstream tasks
    task_instance.xcom_push(key='processed_data', value=data)
    return data


def analyze_results(**context):
    """Retrieve and analyze data from previous task"""
    task_instance = context['task_instance']
    # Pull data from XCom
    data = task_instance.xcom_pull(task_ids='process_data', key='processed_data')
    print(f"Analyzing results: {data}")
    print(f"Total records: {data.get('records_processed', 0)}")
    return "Analysis complete"


def send_notification(**context):
    """Send completion notification"""
    dag_run = context['dag_run']
    print(f"DAG Run ID: {dag_run.run_id}")
    print("Notification: All tasks completed successfully!")
    return "Notification sent"


# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

# Define the DAG
with DAG(
    dag_id='hello_world_advanced',
    default_args=default_args,
    description='An advanced Hello World DAG with parallel tasks',
    schedule_interval='0 8 * * *',  # Run daily at 8 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'hello_world', 'advanced'],
    max_active_runs=1,
    access_control={
        'Restricted': {'can_read', 'can_edit'},
    },
) as dag:

    # Start task
    start = EmptyOperator(
        task_id='start',
    )

    # Greet task
    greet = PythonOperator(
        task_id='greet_user',
        python_callable=greet_user,
        provide_context=True,
    )

    # Parallel bash tasks
    bash_task_1 = BashOperator(
        task_id='echo_hello',
        bash_command='echo "Hello from Bash Task 1"',
    )

    bash_task_2 = BashOperator(
        task_id='echo_world',
        bash_command='echo "World from Bash Task 2"',
    )

    bash_task_3 = BashOperator(
        task_id='print_env',
        bash_command='echo "Environment: $AIRFLOW_HOME"',
    )

    # Data processing task
    process = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
    )

    # Analysis task
    analyze = PythonOperator(
        task_id='analyze_results',
        python_callable=analyze_results,
        provide_context=True,
    )

    # Notification task
    notify = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
        provide_context=True,
    )

    # End task
    end = EmptyOperator(
        task_id='end',
    )

    # Define task dependencies
    # Start -> Greet -> Three parallel bash tasks -> Process -> Analyze -> Notify -> End
    start >> greet >> [bash_task_1, bash_task_2, bash_task_3] >> process >> analyze >> notify >> end
