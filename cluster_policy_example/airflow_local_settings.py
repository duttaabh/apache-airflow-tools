# airflow_local_settings.py

def dag_policy(dag):
    if dag.schedule_interval == "@once":
        raise ValueError("DAGs should not use '@once' as schedule_interval.")
    if not dag.tags:
        dag.tags = ['default']
    print(f"[dag_policy] Processed DAG: {dag.dag_id}")

def task_policy(task):
    print(f"[task_policy] Hello from {__file__}")
    print(f"{task.task_id} retries is set to: {task.retries}")

    if task.retries is None or task.retries != 1:
        task.retries = 1
        print(f"[task_policy] Set default retries for task: {task.task_id}. Setting task.retries = {task.retries}")