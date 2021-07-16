import json
from airflow import DAG
import constants as Constants
from brahma_utils import make_dag

# Default Args
txn_etl_config = json.load(open(Constants.STABLE_TXN_ETL, 'r'))
default_args = txn_etl_config[Constants.DEFAULT_ARGS]

# DAG for TransactionalETL
dag_config = txn_etl_config['txn_etl']
dag_txn_etl = make_dag(dag_config, default_args)