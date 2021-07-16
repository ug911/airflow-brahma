import os
import sys
import argparse
sys.path.insert(1, os.path.expanduser('~/1mg/airflow/db_connect/'))
from athena_pipeline import AthenaPipeline
from s3_pipeline import S3Pipeline


def athena_drop_table(_file):
    for _table in _file:
        _table = _table.replace('\n', '')
        assert _table.__contains__('.'), 'Please add schema name with the table name.'
        query = '''DROP TABLE {}'''.format(_table)
        AthenaPipeline.run_query(query)


def athena_backup_table(_file):
    for _table in _file:
        _table = _table.replace('\n', '')
        assert _table.__contains__('.'), 'Please add schema name with the table name.'
        s3_final_location = 's3://analytics-data-eng/Backup/AthenaTableSchema/{}'\
            .format(_table.replace('.', '_'))
        query = '''SHOW CREATE TABLE {}'''.format(_table)
        execution_id = AthenaPipeline.run_query(query)
        s3_location = AthenaPipeline.get_query_results(execution_id)
        S3Pipeline.mv_s3_to_s3(s3_location, s3_final_location, non_recursive=True)


if __name__ == '__main__':
    # Please update the file athena_operations.txt
    parser = argparse.ArgumentParser(description='Athena operations')
    parser.add_argument('--process_name', action='store', type=str,
                        required=False, help='Process name (drop_table/backup_table)')
    args = parser.parse_args()
    process_name = args.process_name

    operations_file = open('/home/ubuntu/1mg/airflow/utils/athena_operations.txt').readlines()
    if process_name == 'drop_table': athena_drop_table(operations_file)
    if process_name == 'backup_table': athena_backup_table(operations_file)
