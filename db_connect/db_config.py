import json
import os

CONFIG = json.load(open(os.path.expanduser('~/1mg/airflow/configs/psql_config.json')))

# Redshift config
REDSHIFT_CONFIG = CONFIG['Redshift_credentials']
REDSHIFT_HOST = REDSHIFT_CONFIG['host']
REDSHIFT_PORT = REDSHIFT_CONFIG['port']
REDSHIFT_USER = REDSHIFT_CONFIG['user_name']
REDSHIFT_PASSWORD = REDSHIFT_CONFIG['password']
REDSHIFT_DB = REDSHIFT_CONFIG['db_name']
ACCESS_ID = REDSHIFT_CONFIG['aws_access_key_id']
ACCESS_KEY = REDSHIFT_CONFIG['aws_secret_access_key']
IAM_ROLE = REDSHIFT_CONFIG['iam_role']
REDSHIFT_CONNECTION_STRING = "dbname='{}' port='{}' user='{}' " \
                      "password='{}' host='{}'"\
    .format(REDSHIFT_DB, REDSHIFT_PORT, REDSHIFT_USER, REDSHIFT_PASSWORD, REDSHIFT_HOST)


# Mongo config
MONGO_CONFIG = CONFIG['mongo_db']
MONGO_HOST = MONGO_CONFIG['host']
MONGO_PORT = MONGO_CONFIG['port']
MONGO_USER = MONGO_CONFIG['user_name']
MONGO_PASSWORD = MONGO_CONFIG['password']
MONGO_DB = MONGO_CONFIG['db_name']


# Droplet config
DROPLET_CONFIG = CONFIG['droplet_db']
DROPLET_HOST = DROPLET_CONFIG['host']
DROPLET_PORT = DROPLET_CONFIG['port']
DROPLET_USER = DROPLET_CONFIG['user_name']
DROPLET_PASSWORD = DROPLET_CONFIG['password']
DROPLET_DB = DROPLET_CONFIG['db_name']


# DMPL config
DMPL_CONFIG = CONFIG['dmpl_db']
DMPL_HOST = DMPL_CONFIG['host']
DMPL_PORT = DMPL_CONFIG['port']
DMPL_USER = DMPL_CONFIG['user_name']
DMPL_PASSWORD = DMPL_CONFIG['password']
DMPL_DB_DEL = DMPL_CONFIG['db_name_del']
DMPL_DB_DEL_REP = DMPL_CONFIG['db_name_del_replica']
DMPL_DB_GGN = DMPL_CONFIG['db_name_ggn']


# TPPOS config
TPPOS_CONFIG = CONFIG['tppos_db']
TPPOS_HOST = TPPOS_CONFIG['host']
TPPOS_PORT = TPPOS_CONFIG['port']
TPPOS_USER = TPPOS_CONFIG['user_name']
TPPOS_PASSWORD = TPPOS_CONFIG['password']
TPPOS_DB = TPPOS_CONFIG['db_name']


# ODIN config
ODIN_CONFIG = CONFIG['odin_db']
ODIN_HOST = ODIN_CONFIG['host']
ODIN_PORT = ODIN_CONFIG['port']
ODIN_USER = ODIN_CONFIG['user_name']
ODIN_PASSWORD = ODIN_CONFIG['password']
ODIN_DB = ODIN_CONFIG['db_name']
ODIN_CONNECTION_STRING = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
    .format(ODIN_DB, ODIN_PORT, ODIN_USER, ODIN_PASSWORD, ODIN_HOST)


# EMAIL config
EMAIL_CONFIG = CONFIG['analytics_email']
EMAIL = EMAIL_CONFIG['email']
PASSWORD = EMAIL_CONFIG['password']
SENDER = EMAIL_CONFIG['sender']


# Big query config
BQ_CONFIG = json.load(open(os.path.expanduser('~/1mg/airflow/configs/bigquery.json')))
PROJECT_ID = BQ_CONFIG['project_id']


# Admin Service config
ADMIN_CONFIG = CONFIG['admin_service_db']
ADMIN_HOST = ADMIN_CONFIG['host']
ADMIN_PORT = ADMIN_CONFIG['port']
ADMIN_USER = ADMIN_CONFIG['user_name']
ADMIN_PASSWORD = ADMIN_CONFIG['password']
ADMIN_DB = ADMIN_CONFIG['db_name']


# Raven config
RAVEN_CONFIG = CONFIG['raven_db']
RAVEN_HOST = RAVEN_CONFIG['host']
RAVEN_PORT = RAVEN_CONFIG['port']
RAVEN_USER = RAVEN_CONFIG['user_name']
RAVEN_PASSWORD = RAVEN_CONFIG['password']
RAVEN_DB = RAVEN_CONFIG['db_name']


# Wallet config
WALLET_CONFIG = CONFIG['wallet_db']
WALLET_HOST = WALLET_CONFIG['host']
WALLET_PORT = WALLET_CONFIG['port']
WALLET_USER = WALLET_CONFIG['user_name']
WALLET_PASSWORD = WALLET_CONFIG['password']
WALLET_DB = WALLET_CONFIG['db_name']


# Chat bot config
CHAT_BOT_CONFIG = CONFIG['chatbot_db']
CHAT_BOT_HOST = CHAT_BOT_CONFIG['host']
CHAT_BOT_PORT = CHAT_BOT_CONFIG['port']
CHAT_BOT_USER = CHAT_BOT_CONFIG['user_name']
CHAT_BOT_PASSWORD = CHAT_BOT_CONFIG['password']
CHAT_BOT_DB = CHAT_BOT_CONFIG['db_name']


# Order service config
ORDER_SERVICE_CONFIG = CONFIG['order_service_db']
ORDER_SERVICE_HOST = ORDER_SERVICE_CONFIG['host']
ORDER_SERVICE_PORT = ORDER_SERVICE_CONFIG['port']
ORDER_SERVICE_USER = ORDER_SERVICE_CONFIG['user_name']
ORDER_SERVICE_PASSWORD = ORDER_SERVICE_CONFIG['password']
ORDER_SERVICE_DB = ORDER_SERVICE_CONFIG['db_name']

# Data service config
DATA_SERVICE_CONFIG = CONFIG['data_service_db']
DATA_SERVICE_HOST = DATA_SERVICE_CONFIG['host']
DATA_SERVICE_PORT = DATA_SERVICE_CONFIG['port']
DATA_SERVICE_USER = DATA_SERVICE_CONFIG['user_name']
DATA_SERVICE_PASSWORD = DATA_SERVICE_CONFIG['password']

# Taus service config
TAUS_SERVICE_CONFIG = CONFIG['taus_service_db']
TAUS_SERVICE_HOST = TAUS_SERVICE_CONFIG['host']
TAUS_SERVICE_PORT = TAUS_SERVICE_CONFIG['port']
TAUS_SERVICE_USER = TAUS_SERVICE_CONFIG['user_name']
TAUS_SERVICE_PASSWORD = TAUS_SERVICE_CONFIG['password']
TAUS_SERVICE_DB = TAUS_SERVICE_CONFIG['db_name']

# Snowflake config
SNOWFLAKE_CONFIG = CONFIG['snowflake']
SNOWFLAKE_USER = SNOWFLAKE_CONFIG['user_name']
SNOWFLAKE_PASSWORD = SNOWFLAKE_CONFIG['password']
SNOWFLAKE_ACCOUNT = SNOWFLAKE_CONFIG['account']
SNOWFLAKE_ROLE = SNOWFLAKE_CONFIG['role']
SNOWFLAKE_WAREHOUSE = SNOWFLAKE_CONFIG['warehouse']
