# Constants
DEFAULT_ARGS = 'default_args'
RNR_DEFAULT_ARGS = 'rnr_default_args'

# For each dag config
DAG_ARGS = 'args'
DAG_NAME = 'dag_name'
DAG_OWNER = 'owner'
DAG_EMAIL = 'email'
START_DATE = 'start_date'
RETRY_DELAY = 'retry_delay'
SCHEDULE_INTERVAL = 'schedule_interval'
DAG_NUM_STAGES = 'num_stages'
RETRIES = 'retries'
HOST = '13.127.135.185'

# Default Bash Process
DEFAULT_BASH_PROCESS = 'echo "Hello"'

# Process Config
HOME_DIR = 'home_directory'
PREFIX = 'script_language'
TASK_IDS = 'task_ids'
SPLIT_BY = '|'
NO_EXEC_DT_TASKS = 'no_exec_dt_tasks'
NO_SUDO = 'no_sudo'
EXEC_DT = ' --execution_date "{{ execution_date }}"'
BASH_CMDS = 'bash_cmds'
IS_PARALLEL = 'is_parallel'

# Stable Dags
STABLE_METRIC_BOT = 'stable/brahma_metrics.json'
STABLE_HOMEPAGE_METRICS = 'stable/homepage_metrics.json'
STABLE_LOTAME = 'stable/lotame.json'
STABLE_YODA_RAW_LOGS = 'stable/yoda_raw_logs_and_more.json'
STABLE_RX_GZIP = 'stable/rx_gzip.json'
STABLE_DRE = 'stable/dre.json'
STABLE_SKU_OOS = 'stable/sku_oos.json'
STABLE_REVIEWS_RATINGS = 'stable/reviews_ratings.json'
STABLE_YODA_CASSANDRA_LOADER = 'stable/yoda_cassandra_loader.json'
STABLE_RUDDER = 'stable/rudder.json'
STABLE_CATEGORY_RANKING = 'stable/category_ranking.json'
STABLE_DAILY_ETL = 'stable/daily_etl.json'
STABLE_ROLT_SERVICE = 'stable/rolt_service.json'
STABLE_MONTHLY_FIN_BKP = 'stable/monthly_fin_bkp.json'
STABLE_MONTHLY_BKP = 'stable/bkp_monthly.json'
STABLE_LIVE_DSB = 'stable/live_dsb.json'

STABLE_ETL_DMPL = 'stable/etl_dmpl.json'
STABLE_ETL_ODIN = 'stable/etl_odin.json'
STABLE_ETL_TPPOS = 'stable/etl_tppos.json'
STABLE_FLOCK_BOT = 'stable/flock_bot.json'
STABLE_GA_SKU = 'stable/ga_sku.json'
STABLE_MARGIN = 'stable/margin.json'
STABLE_SLOT_AVAILABILITY = 'stable/slot_availability.json'
STABLE_ADOBE_CAMPAIGN = 'stable/adobe_campaign.json'
STABLE_CHECKSUMS = 'stable/checksums.json'
STABLE_ETL_MONGO = 'stable/etl_mongo.json'


# Lab Digitization
STABLE_LAB_REPORT_DIGITIZATION = 'stable/lab_report_digitization.json'
STABLE_LAB_REPORT_DIGITIZATION_PREDIGITIZED = 'stable/lab_report_digitization_predigitized.json'

#Homepage internal cron
STABLE_HOMEPAGE_INTERNAL_CRON = 'stable/homepage_internal_cron.json'


# ADHOC tasks
TRENDING_PRODUCTS = 'stable/trending_products.json'
SEARCH_TAGS = 'stable/search_tags.json'
PDP_PUBLISH = 'stable/pdp_publish.json'

#Allocation ETA cron
STABLE_ALLOCATION_CRON = 'stable/pre_order_eta.json'
STABLE_ALLOCATION_COMBINATION_CRON = 'stable/pre_order_combination_generator.json'

# Inventory.
STABLE_INVENTORY = 'stable/inventory.json'
STABLE_WEEKLY_SUNDAY = 'stable/weekly_sunday.json'
STABLE_RETAIL_STORE = 'stable/retail_store.json'

# Pricing
STABLE_DEM_EXP_DAILY_FILE = 'stable/dem_exp_daily_file.json'
STABLE_DEM_EXP_PRICE_CHANGE = 'stable/dem_exp_price_change.json'
STABLE_DEM_EXP_DATA = 'stable/dem_exp_data.json'
STABLE_PROFIT_QL_HOURLY = 'stable/pricing_profit_ql_hourly.json'
STABLE_PROFIT_DAILY_ATC = 'stable/pricing_profit_daily_atc.json'
STABLE_PROFIT_DAILY_REWARDS = 'stable/pricing_profit_ql_rewards.json'
STABLE_CURRENT_PRICE_CHECKER = 'stable/pricing_current_price_checker.json'

# Auto emailers
STABLE_AUTO_EMAIL = 'stable/auto_email.json'

# Cart filler
STABLE_CART_FILLER_METRICS = 'stable/cart_filler_metrics.json'
STABLE_CART_FILLER_CASSANDRA = 'stable/cart_filler_cassandra.json'
STABLE_CART_FILLER_BOT = 'stable/cart_filler_bot.json'

# Logs cleanup
STABLE_AIRFLOW_LOGS_CLEANUP = 'stable/airflow_logs_cleanup.json'

# Transactional ETL
STABLE_TXN_NONDMS_ETL = 'stable/txn_nondms_etl.json'
STABLE_TXN_DELTA_ETL = 'stable/txn_delta_etl.json'
STABLE_TXN_ETL = 'stable/txn_etl.json'
STABLE_TXN_DCP = 'stable/txn_dcp.json'
STABLE_TXN_DROPLET = 'stable/txn_droplet.json'
STABLE_TXN_GRE = 'stable/txn_gre.json'
STABLE_TXN_UNLOAD = 'stable/txn_unload.json'
STABLE_TXN_PROCUREMENT = 'stable/txn_procurement.json'

# Covid Trends
STABLE_COVID_TRENDS = 'stable/covid_trends.json'

# Aggregate Survey Results

AGGREGATE_SURVEY_RESULTS = 'stable/aggregate_survey_result.json'
