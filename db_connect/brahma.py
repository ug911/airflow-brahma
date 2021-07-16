"""
--> Brahma
"""
import os
import re
import argparse
from datetime import datetime, timedelta

from athena_pipeline import AthenaPipeline
from redshift_pipeline import RedshiftPipeline
from version_pipeline import VersionPipeline
from s3_pipeline import S3Pipeline
from snowflake_pipeline import SnowflakePipeline
from email_pipeline import EmailPipeline


# TODO: Validation of arguments based on the process type - Done
# TODO: Individual files for individual classes - Later
# TODO: Ability to call python functions directly using PythonOperator
# TODO: Create a dummy dag with x number of stages. Run a dummy task in each stage
# TODO: Find dag based on dag name, keyword search type - Done

class Brahma:
    """
    Brahma is a parent class that incorporates different classes
    """
    @staticmethod
    def function_map():
        """
        Function map
        """
        process_function_map = {
            'process_start': Brahma.process_status,
            'athena_query': Brahma.athena_query,
            'athena_dq': Brahma.athena_dq,
            'athena_mdq': Brahma.athena_mdq,
            'athena_repair': Brahma.athena_repair,
            'athena_download': Brahma.athena_download,
            'athena_drop': Brahma.athena_drop,
            'athena_query_move': Brahma.athena_query_move,
            'copy_redshift': Brahma.copy_redshift,
            'copy_redshift_version': Brahma.copy_redshift_version,
            'alter_redshift': Brahma.alter_redshift,
            'alter_redshift_lock': Brahma.alter_redshift_lock,
            'truncate_redshift': Brahma.truncate_redshift,
            'etl_redshift': Brahma.etl_redshift,
            's3_del': Brahma.s3_del,
            's3_cp': Brahma.s3_cp,
            's3_move': Brahma.s3_move,
            'unload_s3_parquet': Brahma.unload_s3_parquet,
            'unload_s3_csv': Brahma.unload_s3_csv,
            'del_unload_s3': Brahma.del_unload_s3,
            'build_error_check': Brahma.build_error_check,
            'grant_access': Brahma.grant_access,
            'sf_query': Brahma.sf_query,
            'sf_dq': Brahma.sf_dq,
            'sf_tq': Brahma.sf_tq,
            'sf_tc': Brahma.sf_tc,
            'sf_alter_operations': Brahma.sf_alter_operations,
            'sf_truncate_copy_csv': Brahma.sf_truncate_copy_csv,
            'mongodump_bson_json': Brahma.mongodump_bson_json,
            'local_gzip_to_athena_table': Brahma.local_gzip_to_athena_table
        }
        return process_function_map

    @staticmethod
    def athena_query():
        """
        Run a query on athena
        """
        alert_message = "Process = athena_query\nStatus = Failed\n" \
                        "Required arguments = 'query_filename', 'ymd', " \
                        "'execution_date'"
        assert query_filename and ymd and execution_date, \
            alert_message
        AthenaPipeline.athena_query(query_filename, ymd, execution_date, check_s3_bucket,
                                    check_s3_key, replace_ymd_by_regex, int(date_counter))

    @staticmethod
    def athena_dq():
        """
        Drop table on athena and run query on athena
        """
        alert_message = "Process = athena_dq\nStatus = Failed\n" \
                        "Required arguments = 'query_filename', 'athena_table', " \
                        "'execution_date'"
        assert query_filename and athena_table and execution_date, alert_message
        AthenaPipeline.athena_dq(query_filename, athena_table, execution_date)

    @staticmethod
    def athena_mdq():
        """
        Move the s3 location to a temp location, drop table on athena, and
        run query on athena
        """
        alert_message = "Process = athena_mdq\nStatus = Failed\n" \
                        "Required arguments = 'query_filename', 'athena_table', " \
                        "'path_category', 'path_name'"
        assert query_filename and athena_table and path_category and path_name, \
            alert_message
        AthenaPipeline.athena_mdq(query_filename, athena_table, path_category, path_name)

    @staticmethod
    def athena_repair():
        """
        Repair a table on athena
        """
        alert_message = "Process = athena_repair\nStatus = Failed\n" \
                        "Required arguments = 'athena_table', 's3_path', 'execution_date'"
        assert athena_table and s3_path and execution_date, alert_message
        AthenaPipeline.athena_repair(athena_table, s3_path, execution_date, additional_partition)

    @staticmethod
    def athena_download():
        """
        Download athena query results from athena
        """
        alert_message = "Process = athena_download\nStatus = Failed\n" \
                        "Required arguments = 'query_filename', 'download_directory', " \
                        "'download_filename'"
        assert query_filename and download_directory and download_filename, alert_message
        AthenaPipeline.athena_download(query_filename, download_directory, download_filename)

    @staticmethod
    def athena_drop():
        """
        Drop table on athena.
        """
        alert_message = "Process = athena_drop\nStatus = Failed\n" \
                        "Required arguments = 'athena_table'"
        assert athena_table, alert_message
        AthenaPipeline.drop_table(athena_table, execution_date)

    @staticmethod
    def athena_query_move():
        """
        Move athena query results to a different s3 location
        """
        alert_message = "Process = athena_query_move\nStatus = Failed\n" \
                        "Required arguments = 'query_filename', 's3_path'"
        assert query_filename and s3_path, alert_message
        _s3_path = s3_path
        if split_version and split_version == 'current_ymd' and download_filename:
            new_execution_date = str(datetime.now() +
                                     timedelta(minutes=330)).split(' ')[0]
            year, month, day, hour, minute = VersionPipeline\
                .create_date_version(new_execution_date)
            version = '{}_{}_{}'.format(year, month, day)
            _s3_path = s3_path + download_filename + version + '.csv'
        AthenaPipeline.athena_query_move(query_filename, _s3_path, non_recursive)

    @staticmethod
    def copy_redshift():
        """
        Copy data into redshift from s3 in parquet format
        """
        alert_message = "Process = copy_redshift\nStatus = Failed\n" \
                        "Required arguments = 'redshift_schema', 'redshift_table', 's3_path'"
        assert redshift_schema and redshift_table and s3_path, alert_message
        RedshiftPipeline.copy_s3_redshift_parquet(redshift_schema, redshift_table, s3_path)

    @staticmethod
    def copy_redshift_version():
        """
        Copy data into redshift from versioned s3 in parquet format
        """
        alert_message = "Process = copy_redshift_version\nStatus = Failed\n" \
                        "Required arguments = 'redshift_schema', 'redshift_table', 's3_path', " \
                        "'execution_date'"
        assert redshift_schema and redshift_table and s3_path and execution_date, alert_message
        year, month, day, hour, minute = VersionPipeline.create_date_version(execution_date)
        version = '{}_{}_{}_{}_{}'.format(year, month, day, hour, minute)
        _s3_path = re.sub('{version}', version, s3_path)
        RedshiftPipeline.copy_s3_redshift_parquet(redshift_schema, redshift_table, _s3_path)

    @staticmethod
    def alter_redshift():
        """
        Alter table on redshift
        """
        alert_message = "Process = alter_redshift\nStatus = Failed\n" \
                        "Required arguments = 'redshift_schema', 'redshift_table', " \
                        "'redshift_new_table'"
        assert redshift_schema and redshift_table and redshift_new_table, alert_message
        RedshiftPipeline.alter_table(redshift_schema, redshift_table, redshift_new_table)

    @staticmethod
    def alter_redshift_lock():
        """
        Remove the lock from table and alter table on redshift
        """
        alert_message = "Process = alter_redshift_lock\nStatus = Failed\n" \
                        "Required arguments = 'redshift_schema', 'redshift_table', " \
                        "'redshift_new_table'"
        assert redshift_schema and redshift_table and redshift_new_table, alert_message
        RedshiftPipeline.alter_redshift_lock(redshift_schema, redshift_table,
                                             redshift_new_table)

    @staticmethod
    def truncate_redshift():
        """
        Truncate table on redshift
        """
        alert_message = "Process = truncate_redshift\nStatus = Failed\n" \
                        "Required arguments = 'redshift_schema', 'redshift_table'"
        assert redshift_schema and redshift_table, alert_message
        RedshiftPipeline.truncate_table(redshift_schema, redshift_table)

    @staticmethod
    def etl_redshift():
        """
        Run a query on redshift
        """
        alert_message = "Process = etl_redshift\nStatus = Failed\n" \
                        "Required arguments = 'query_file_path'"
        assert query_file_path, alert_message
        RedshiftPipeline.etl_redshift(query_file_path)

    @staticmethod
    def s3_del():
        """
        Delete files from s3
        """
        alert_message = "Process = s3_del\nStatus = Failed\n" \
                        "Required arguments = 's3_bucket', 's3_path'"
        assert s3_bucket and s3_path, alert_message
        _s3_path = 's3://{}/{}'.format(s3_bucket, s3_path)
        S3Pipeline.drop_s3_file(_s3_path)

    @staticmethod
    def s3_cp():
        """
        Copy files from one S3 location to another
        """
        alert_message = "Process = s3_cp\nStatus = Failed\n" \
                        "Required arguments = 's3_path', 's3_path_final', 'execution_date'"
        assert s3_path and s3_path_final and execution_date, alert_message
        if split_version and split_version == 'current_ymd':
            new_execution_date = str(datetime.now() +
                                     timedelta(minutes=330)).split(' ')[0]
            year, month, day, hour, minute = VersionPipeline\
                .create_date_version(new_execution_date)
            version = '{}_{}_{}'.format(year, month, day)
        else:
            year, month, day, hour, minute = VersionPipeline\
                .create_date_version(execution_date)
            version = '{}_{}_{}_{}_{}'.format(year, month, day, hour, minute)
        _s3_path_final = re.sub('{version}', version, s3_path_final)
        S3Pipeline.cp_s3_to_s3(s3_path, _s3_path_final)

    @staticmethod
    def s3_move():
        """
        Move files from one S3 location to another
        """
        alert_message = "Process = s3_move\nStatus = Failed\n" \
                        "Required arguments = 's3_path', 's3_path_final', " \
                        "'execution_date'"
        assert s3_path and s3_path_final and execution_date, alert_message
        year, month, day, hour, minute = VersionPipeline.create_date_version \
            (execution_date)
        version = '{}_{}_{}_{}_{}'.format(year, month, day, hour, minute)
        _s3_path_final = re.sub('{version}', version, s3_path_final)
        S3Pipeline.mv_s3_to_s3(s3_path, _s3_path_final)

    @staticmethod
    def unload_s3_parquet():
        """
        Unload redshift data to an S3 location in parquet format
        """
        alert_message = "Process = unload_s3_parquet\nStatus = Failed\n" \
                        "Required arguments = 'redshift_schema', 'redshift_table', " \
                        "'s3_path', 'execution_date'"
        assert redshift_schema and redshift_table and s3_path and execution_date, \
            alert_message
        if split_version and split_version == 'current_ymd':
            new_execution_date = str(datetime.now() +
                                     timedelta(minutes=330)).split(' ')[0]
            year, month, day, hour, minute = VersionPipeline.create_date_version \
                (new_execution_date)
            version = '{}_{}_{}'.format(year, month, day)
        else:
            year, month, day, hour, minute = VersionPipeline.create_date_version \
                (execution_date)
            version = '{}_{}_{}_{}_{}'.format(year, month, day, hour, minute)
        _s3_path = re.sub('{version}', version, s3_path)
        RedshiftPipeline.unload_table_s3_parquet(redshift_schema, redshift_table, _s3_path)

    @staticmethod
    def unload_s3_csv():
        """
        Unload redshift data to an S3 location in CSV format
        """
        _s3_path = s3_path
        alert_message = "Process = unload_s3_csv\nStatus = Failed\n" \
                        "Required arguments = 'redshift_schema', 'redshift_table', " \
                        "'s3_path', 'execution_date'"
        assert redshift_schema and redshift_table and s3_path and execution_date, \
            alert_message
        if split_version and split_version == 'current_ymd':
            new_execution_date = str(datetime.now() + timedelta(minutes=330)).split(' ')[0]
            year, month, day, hour, minute = VersionPipeline.create_date_version \
                (new_execution_date)
            version = '{}_{}_{}'.format(year, month, day)
            _s3_path = re.sub('{version}', version, s3_path)
        RedshiftPipeline.unload_table_s3_csv(redshift_schema, redshift_table, _s3_path,
                                             col_string)

    @staticmethod
    def del_unload_s3():
        """
        Delete s3 location and unload redshift data to an S3 location in parquet format
        """
        alert_message = "Process = del_unload_s3\nStatus = Failed\n" \
                        "Required arguments = 'redshift_schema', 'redshift_table', 's3_path'"
        assert redshift_schema and redshift_table and s3_path, alert_message
        S3Pipeline.drop_s3_file(s3_path)
        RedshiftPipeline.unload_table_s3_parquet(redshift_schema, redshift_table, s3_path)

    @staticmethod
    def build_error_check():
        """
        Error check in Builds (DeltaBuild and FullBuild)
        """
        alert_message = "Process = build_error_check\nStatus = Failed\n" \
                        "Required arguments = 'dag_name', 'execution_date'"
        assert dag_name and execution_date, alert_message
        _path_name = os.path.expanduser('~/airflow/logs/{}'.format(dag_name))
        failed_task_list = list()

        for task_id in ["altr_order_flow", "altr_order_flow_temp",
                        "altr_order_flow_temp_replmnt",
                        "cp_order_sales", "altr_order_sales",
                        "altr_order_sales_temp",
                        "altr_order_sales_temp_replmnt", "cp_order_sku_sales",
                        "altr_order_sku_sales",
                        "altr_order_sku_sales_temp", "altr_order_sku_sales_temp_replmnt"]:
            with open("{}/{}/{}/1.log".format(_path_name, task_id, str(execution_date)),
                      'r') as log_file:
                for line in log_file:
                    if 'SSL SYSCALL' in line:
                        failed_task_list.append(task_id)

        if failed_task_list:
            EmailPipeline.send_mail_without_attachment\
                ("Tasks which didn't run properly in DAG: ".format(dag_name),
                 str(failed_task_list), ["sarthak.jain@1mg.com", "prince.mishra@1mg.com"])
        else:
            EmailPipeline.send_mail_without_attachment\
                ("{} - Everything ran successfully".format(dag_name),
                 str(failed_task_list), ["sarthak.jain@1mg.com", "prince.mishra@1mg.com"])

    @staticmethod
    def grant_access():
        """
        Grant access on redshift
        """
        alert_message = "Process = grant_access\nStatus = Failed\n" \
                        "Required arguments = 'query_file_path'"
        assert query_file_path, alert_message
        RedshiftPipeline.grant_access(query_file_path)

    @staticmethod
    def process_status():
        """
        Mark starting/ending of an airflow DAG.
        """
        alert_message = "Process = process_status\nStatus = Failed\n" \
                        "Required arguments = 'process_marker'"
        assert process_marker, alert_message
        print("Process - {} at {}".format(process_marker, datetime.now()))

    @staticmethod
    def sf_query():
        """
        Run a query on snowflake.
         """
        alert_message = "Process = sf_query\nStatus = Failed\n" \
                        "Required arguments = 'query_filename', 'execution_date'"
        assert query_filename and execution_date, alert_message
        SnowflakePipeline.sf_query(query_filename, execution_date, sf_connect_type, sf_db, sf_schema)

    @staticmethod
    def sf_dq():
        """
        Drop table on snowflake and run query on snowflake
         """
        alert_message = "Process = sf_dq\nStatus = Failed\n" \
                        "Required arguments = 'query_filename', 'sf_table'"
        assert query_filename and sf_table and str(sf_table).count('.') == 2, \
            alert_message
        SnowflakePipeline.sf_dq(query_filename, sf_table)

    @staticmethod
    def sf_tq():
        """
        Truncate table on snowflake and run query on snowflake
        """
        alert_message = "Process = sf_tq\nStatus = Failed\n" \
                        "Required arguments = 'query_filename', 'sf_table'"
        assert query_filename and sf_table and str(sf_table).count('.') == 2, \
            alert_message
        SnowflakePipeline.sf_tq(query_filename, sf_table)

    @staticmethod
    def sf_tc():
        """
        Truncate table on snowflake and copy data into snowflake table from s3 location.
        """
        alert_message = "Process = sf_tc\nStatus = Failed\n" \
                        "Required arguments = 'query_filename', 'sf_db', 'sf_schema', " \
                        "'sf_table'"
        assert query_filename and sf_db and sf_schema and sf_table and \
               str(sf_table).count('.') == 2, alert_message
        SnowflakePipeline.sf_tc(query_filename, sf_db, sf_schema, sf_table, execution_date)

    @staticmethod
    def sf_alter_operations():
        """
        Refresh external table in snowflake.
        """
        alert_message = "Process = sf_alter_operations\nStatus = Failed\n" \
                        "Required arguments = 'alter_process_name'"
        assert alter_process_name, alert_message
        SnowflakePipeline.sf_alter_operations(alter_process_name, sf_table, warehouse_name, warehouse_size)

    @staticmethod
    def sf_truncate_copy_csv():
        """
        Truncate table in snowflake and copy a csv file from s3.
        """
        alert_message = "Process = sf_truncate_copy_csv\nStatus = Failed\n" \
                        "Required arguments = 'sf_table', 's3_path', 'delimiter', " \
                        "'skip_header'"
        assert sf_table and str(sf_table).count('.') == 2 and s3_path and delimiter and skip_header,\
            alert_message
        SnowflakePipeline.sf_truncate_copy_csv(sf_table, s3_path, delimiter, skip_header)
    
    
    @staticmethod
    def local_gzip_to_athena_table():
        """
        Gzip a local file, move it to s3 bucket, run a DDL command to create a table (optional), 
        followed by a CTAS for updating an Athena Table
        """
        alert_message = "Process = local_gzip_to_athena_table\nStatus = Failed\n" \
                        "Required arguments = 'localFile', 's3Path', 'delimiter', " \
                        "'skip_header'"
        assert sf_table and str(sf_table).count('.') == 2 and s3_path and delimiter and skip_header,\
            alert_message
        # TODO: GZIP local file
        # "gzip {}".format(localFile)
        # "s3cmd put {}.gz s3://{}".format(localFile, s3Path)
        # athena_...


if __name__ == '__main__':
    parser = argparse.ArgumentParser\
        (description='Move on S3, Drop in Athena and Run Query in Athena')
    parser.add_argument('--process_type', action='store', type=str,
                        required=True, help='Process to execute on athena')
    parser.add_argument('--query_filename', action='store', type=str,
                        required=False, help='Query')
    parser.add_argument('--athena_schema', action='store', type=str,
                        required=False, help='Athena Schema')
    parser.add_argument('--athena_table', action='store', type=str,
                        required=False, help='Athena Table')
    parser.add_argument('--redshift_schema', action='store', type=str,
                        required=False, help='Redshift Schema')
    parser.add_argument('--redshift_table', action='store', type=str,
                        required=False, help='Redshift Table')
    parser.add_argument('--redshift_new_table', action='store', type=str,
                        required=False, help='Redshift New Table')
    parser.add_argument('--path_category', action='store', type=str,
                        required=False, help='Path Category to Delete')
    parser.add_argument('--path_name', action='store', type=str,
                        required=False, help='Path to Delete')
    parser.add_argument('--check_s3_bucket', action='store', type=str,
                        required=False, default='', help='Check s3 bucket?')
    parser.add_argument('--check_s3_key', action='store', type=str,
                        required=False, default='', help='Check s3 key?')
    parser.add_argument('--ymd', action='store', type=int,
                        required=False, default=1, help='Add Year, Month, Day')
    parser.add_argument('--replace_ymd_by_regex', action='store', type=str,
                        required=False, default='', help='regex')
    parser.add_argument('--download_filename', action='store', type=str,
                        required=False, help='Download Filename')
    parser.add_argument('--download_directory', action='store', type=str,
                        required=False, default='data', help='Download Directory')
    parser.add_argument('--s3_bucket', action='store', type=str,
                        required=False, default=None, help='S3 bucket')
    parser.add_argument('--s3_path', action='store', type=str,
                        required=False, default=None, help='S3 Location to Store Output')
    parser.add_argument('--s3_path_final', action='store', type=str,
                        required=False, default=None, help='Final S3 Location')
    parser.add_argument('--query_file_path', action='store', type=str,
                        required=False, default=None, help='Query file path')
    parser.add_argument('--date_counter', action='store', type=str,
                        required=False, default=-1,
                        help='Date counter to run query at a historical date')
    parser.add_argument('--execution_date', action='store', type=str,
                        required=False, help='Execution Date', default='')
    parser.add_argument('--split_version', action='store', type=str,
                        required=False, help='Split version')
    parser.add_argument('--col_string', action='store', type=str,
                        required=False, default='*', help='Column string')
    parser.add_argument('--non_recursive', action='store', type=str,
                        required=False, help='s3 to s3 non recursive transfer')
    parser.add_argument('--additional_partition', action='store', type=str,
                        required=False, help='Additional partition to repair')
    parser.add_argument('--dag_name', action='store', type=str,
                        required=False, help='Name of the dag')
    parser.add_argument('--process_marker', action='store', type=str,
                        required=False, help='Mark starting/ending of an airflow DAG')
    # Snowflake parameters
    parser.add_argument('--sf_db', action='store', type=str,
                        required=False, default='', help='Name of snowflake database')
    parser.add_argument('--sf_schema', action='store', type=str,
                        required=False, default='', help='Name of snowflake schema')
    parser.add_argument('--sf_table', action='store', type=str,
                        required=False, default='', help='Name of snowflake table')
    parser.add_argument('--sf_connect_type', action='store', type=str,
                        required=False, default='warehouse', help='Type of snowflake connect')
    parser.add_argument('--delimiter', action='store', type=str,
                        required=False, default=',', help='CSV file delimiter')
    parser.add_argument('--skip_header', action='store', type=int,
                        required=False, default=1, help='Skip header row (0/1)')
    parser.add_argument('--alter_process_name', action='store', type=str,
                        required=False, help='Alter process name on snowflake')
    parser.add_argument('--warehouse_name', action='store', type=str,
                        required=False, default='compute_wh', help='Warehouse name of snowflake')
    parser.add_argument('--warehouse_size', action='store', type=str,
                        required=False, default='xsmall', help='Warehouse size of snowflake')

    args = parser.parse_args()
    process_marker = args.process_marker
    process_type = args.process_type
    query_filename = args.query_filename
    athena_schema = args.athena_schema
    athena_table = args.athena_table
    redshift_schema = args.redshift_schema
    redshift_table = args.redshift_table
    redshift_new_table = args.redshift_new_table
    path_category = args.path_category
    path_name = args.path_name
    check_s3_bucket = args.check_s3_bucket
    check_s3_key = args.check_s3_key
    ymd = args.ymd
    download_directory = args.download_directory
    download_filename = args.download_filename
    s3_bucket = args.s3_bucket
    s3_path = args.s3_path
    s3_path_final = args.s3_path_final
    query_file_path = args.query_file_path
    replace_ymd_by_regex = args.replace_ymd_by_regex
    date_counter = args.date_counter
    split_version = args.split_version
    col_string = args.col_string
    execution_date = args.execution_date
    additional_partition = args.additional_partition
    dag_name = args.dag_name
    if args.non_recursive:
        non_recursive = True
    else:
        non_recursive = False

    sf_db = args.sf_db
    sf_schema = args.sf_schema
    sf_table = args.sf_table
    sf_connect_type = args.sf_connect_type
    delimiter = args.delimiter
    skip_header = args.skip_header
    alter_process_name = args.alter_process_name
    warehouse_name = args.warehouse_name
    warehouse_size = args.warehouse_size

    Brahma.function_map().get(process_type, lambda: "Invalid month")()
