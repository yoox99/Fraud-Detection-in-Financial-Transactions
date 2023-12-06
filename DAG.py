from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# Define default_args dictionary to specify the DAG's default parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'fraud_detection_workflow',
    default_args=default_args,
    description='Automated Fraud Detection Workflow',
    schedule_interval=timedelta(days=1),  # Adjust the interval as needed
)

# Task 1: Fetch Data from API
fetch_data_task = BashOperator(
    task_id='fetch_data',
    bash_command='python data.py',  
    dag=dag,
)

# Task 2: Apply Transformations and Insert Data into Hive
transform_insert_task = BashOperator(
    task_id='transform_insert_data',
    bash_command='python trans_insert.py',  
    dag=dag,
)

# Task 3: Check for Fraud Transactions
check_fraud_task = BashOperator(
    task_id='check_fraud',
    bash_command='python hive_queries.py',  
    dag=dag,
)

# Define the task dependencies
fetch_data_task >> transform_insert_task >> check_fraud_task
