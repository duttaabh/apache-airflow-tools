"""
Simple Hello World DAG for Apache Airflow 2.10.3

This DAG demonstrates basic task execution with PythonOperator
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def print_hello():
    """Simple function to print hello message"""
    print("Hello World from Airflow!")
    return "Hello World"


def print_date():
    """Function to print current date and time"""
    current_time = datetime.now()
    print(f"Current date and time: {current_time}")
    return current_time


def print_goodbye():
    """Function to print goodbye message"""
    print("Goodbye! Task completed successfully.")
    return "Goodbye"


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='hello_world_simple',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'hello_world'],
) as dag:

    # Task 1: Print Hello
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    # Task 2: Print Date
    date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )

    # Task 3: Print Goodbye
    goodbye_task = PythonOperator(
        task_id='print_goodbye',
        python_callable=print_goodbye,
    )

    # Define task dependencies
    hello_task >> date_task >> goodbye_task
