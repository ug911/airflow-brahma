"""
--> SnowflakePipeline
"""
import re
import snowflake.connector
from datetime import datetime
from version_pipeline import VersionPipeline
from db_config import SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, \
    SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE


class SnowflakePipeline:
    """
    Snowflake pipeline is a class that stores all the snowflake database related functions
    """
    @staticmethod
    def sf_warehouse_connect():
        """
        Snowflake warehouse connect.
        """
        con = snowflake.connector.connect(user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD,
                                          account=SNOWFLAKE_ACCOUNT, role=SNOWFLAKE_ROLE,
                                          warehouse=SNOWFLAKE_WAREHOUSE)
        cursor = con.cursor()
        return cursor

    @staticmethod
    def sf_schema_connect(sf_db, sf_schema):
        """
        Snowflake schema connect.
        """
        con = snowflake.connector.connect(user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD,
                                          account=SNOWFLAKE_ACCOUNT, role=SNOWFLAKE_ROLE,
                                          warehouse=SNOWFLAKE_WAREHOUSE,
                                          database=sf_db, schema=sf_schema)
        cursor = con.cursor()
        return cursor

    @staticmethod
    def run_query(query, execution_date='', sf_connect_type='warehouse', sf_db='', sf_schema=''):
        """
        Run a query on snowflake.
        Args:
            execution_date ():
            sf_connect_type ():
            sf_schema ():
            sf_db ():
            query ():
        """
        print(query)
        if not query.endswith(';'): query += ';'

        cursor = ''
        if sf_connect_type == 'warehouse': cursor = SnowflakePipeline.sf_warehouse_connect()
        elif sf_db and sf_schema and sf_connect_type == 'schema':
            cursor = SnowflakePipeline.sf_schema_connect(sf_db, sf_schema)
        assert cursor, "Please add proper arguments for the snowflake cursor"

        if execution_date:
            try:
                edt = datetime.strptime(execution_date[:10], '%Y-%m-%d')
            except Exception as exp:
                print(exp)
                try:
                    edt = datetime.strptime(execution_date, '%Y-%m-%d %H:%M:%S')
                except Exception as exp:
                    print(exp)
                    edt = datetime.strptime(execution_date.split('T')[0], '%Y-%m-%d')
            _year, _month, _day = edt.year, '{:02d}'.format(edt.month), '{:02d}'.format(edt.day)

            eval_year, eval_month, eval_day = edt.year, edt.month, edt.day
            year, month, day, hour, minute = \
                VersionPipeline.create_date_version(execution_date)
            version = '{}_{}_{}_{}_{}'.format(year, month, day, hour, minute)
            query = query.format(eval_year=eval_year, eval_month=eval_month,
                                 eval_day=eval_day, version=version)

        cursor.execute(query)
        cursor.execute("COMMIT;")
        exec_id = cursor.sfqid
        return exec_id

    @staticmethod
    def get_query_results(exec_id):
        """
        Get query results from snowflake.
        Args:
            exec_id ():
        """
        print(exec_id)
        cursor = SnowflakePipeline.sf_warehouse_connect()
        cursor.get_results_from_sfqid(exec_id)
        results = cursor.fetchall()
        print(results)
        return results

    @staticmethod
    def drop_table(sf_table):
        """
        Drop table on Snowflake
        Args:
            sf_table ():
        """
        query = "DROP TABLE IF EXISTS {sf_table};" \
            .format(sf_table=sf_table)
        SnowflakePipeline.run_query(query)
        print('Done - Dropping table')

    @staticmethod
    def truncate_table(sf_table):
        """
        Truncate table on Snowflake
        Args:
            sf_table ():
        """
        query = "TRUNCATE TABLE IF EXISTS {sf_table};" \
            .format(sf_table=sf_table)
        SnowflakePipeline.run_query(query)
        print('Done - Truncate table')

    @staticmethod
    def sf_alter_operations(alter_process_name, sf_table, warehouse_name, warehouse_size):
        """
        Alter commands of snowflake.
        Args:
            warehouse_name ():
            warehouse_size ():
            alter_process_name ():
            sf_table ():
        """
        operation_map = {
            'refresh_external_table': "ALTER EXTERNAL TABLE IF EXISTS {sf_table} REFRESH;"
                .format(sf_table=sf_table),
            'change_warehouse_size': "ALTER WAREHOUSE {warehouse_name} SET WAREHOUSE_SIZE = '{warehouse_size}' "
                                     "AUTO_SUSPEND = 30 AUTO_RESUME = TRUE COMMENT = '';"
                .format(warehouse_name=warehouse_name, warehouse_size=warehouse_size)
        }
        query = operation_map.get(alter_process_name, '')
        if query:
            SnowflakePipeline.run_query(query)
        assert query, "Please add proper alter operation name"

    @staticmethod
    def sf_query(query_filename, execution_date, sf_connect_type, sf_db, sf_schema):
        query = open('/home/ubuntu/1mg/airflow/tasks/queries/snowflake/' +
                     query_filename, 'r').read()
        SnowflakePipeline.run_query(query, sf_connect_type=sf_connect_type,
                                    sf_db=sf_db, sf_schema=sf_schema)

    @staticmethod
    def sf_dq(query_filename, sf_table):
        """
        1. Drop a table on Snowflake.
        2. Run a query on Snowflake.
        Args:
            query_filename ():
            sf_table ():
        """
        SnowflakePipeline.drop_table(sf_table)

        query = open('/home/ubuntu/1mg/airflow/tasks/queries/snowflake/' +
                     query_filename, 'r').read()
        SnowflakePipeline.run_query(query)

    @staticmethod
    def sf_tc(query_filename, sf_db, sf_schema, sf_table, execution_date):
        """
        1. Truncate a table on Snowflake.
        2. Copy data into snowflake table from s3 location.
        Args:
            execution_date ():
            query_filename ():
            sf_db ():
            sf_schema ():
            sf_table ():
        """
        SnowflakePipeline.truncate_table(sf_table)
        query = open('/home/ubuntu/1mg/airflow/tasks/queries/snowflake/' +
                     query_filename, 'r').read()
        SnowflakePipeline.run_query(query, execution_date=execution_date,
                                    sf_connect_type='schema', sf_db=sf_db,
                                    sf_schema=sf_schema)

    @staticmethod
    def sf_truncate_copy_csv(sf_table, s3_path, delimiter, skip_header):
        SnowflakePipeline.truncate_table(sf_table)
        query = "COPY INTO {sf_table} FROM '{s3_path}' STORAGE_INTEGRATION = s3_connection " \
                "FILE_FORMAT = (FIELD_DELIMITER = '{delimiter}', SKIP_HEADER = {skip_header});"\
            .format(sf_table=sf_table, s3_path=s3_path, delimiter=delimiter,
                    skip_header=skip_header)
        SnowflakePipeline.run_query(query)
