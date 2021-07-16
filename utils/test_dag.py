from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

'''
'''
datetime_obj = datetime

now_date = datetime.now() + timedelta(-1)

## Define the DAG object
default_args = {
    'owner': 'Utkarsh Gupta',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 1),
    'retries': 2,
    'email':['anand.agrawal@1mg.com', 'utkarsh.gupta@1mg.com'],
    'email_on_failure':True,
    'email_on_retry':False,
    'retry_delay': timedelta(minutes=2),
    'max_active_runs':30,
}

dag = DAG('test_scheduler_daily', catchup=False, default_args=default_args, schedule_interval='30 0 * * *')

task = BashOperator(
    task_id='task1',
    bash_command='echo "Hello"',
    dag=dag)

dag2 = DAG('test_scheduler_hourly', catchup=False, default_args=default_args, schedule_interval='15 * * * *')

task2 = BashOperator(
    task_id='task1',
    bash_command='echo "Hello"',
    dag=dag2)

dag3 = DAG('random_dag', catchup=False, default_args=default_args, schedule_interval='15 * * * *')

task3 = BashOperator(
    task_id='task3',
    bash_command='echo "Hello"',
    dag=dag3)


