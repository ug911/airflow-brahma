from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.models import DAG
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 3, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def return_branch(**kwargs):

    branches = ['branch_0,''branch_1', 'branch_2', 'branch_3', 'branch_4']

    return random.choice(branches)

dag = DAG('test_branch_dag', catchup=False, default_args=default_args, schedule_interval='10 * * * *')

kick_off_dag = DummyOperator(task_id='run_this_first', dag=dag)

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=return_branch,
    provide_context=True, 
    dag=dag)

kick_off_dag >> branching

for i in range(0, 5):
    d = DummyOperator(task_id='branch_{0}'.format(i), dag=dag)
    for j in range(0, 3):
        m = DummyOperator(task_id='branch_{0}_{1}'.format(i, j), dag=dag)
        d >> m
    branching >> d
