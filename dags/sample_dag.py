import json
from airflow import DAG
import constants as Constants
from brahma_utils import make_dag

dag_json = json.load(open('/home/ubuntu/1mg/airflow/dags/stable/sample_dag.json', 'r'))
default_args = dag_json[Constants.DEFAULT_ARGS]

dag_config = dag_json['dag_file']
dag = make_dag(dag_config, default_args)
