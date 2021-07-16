#!/usr/bin/env python3
import json
import boto3
import pandas as pd
import argparse
from datetime import datetime
from cp_red import run_redshift_query, update_red

def check_red(table_name, y, m, d, access_key, secret_key, db_config, storage_table='athena_checksums.daily_counts', store_flag=1):
    athena_q = '''SELECT year, month, day, '{t}' as table_name, count(*) as counts FROM {t} 
        GROUP BY 1, 2, 3
        ORDER BY 1 DESC, 2 DESC, 3 DESC '''.format(t=table_name)
    del_q = '''DELETE FROM {s} WHERE table_name = '{t}' '''.format(t=table_name, s=storage_table)
    copy_q = ''' COPY {s}'''.format(s=storage_table) + \
             ''' FROM '{}' WITH CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
        CSV QUOTE AS '"' DELIMITER ',' IGNOREHEADER 1 MAXERROR 100; '''
    if store_flag:
        update_red(athena_q, del_q, copy_q, access_key, secret_key, db_config)

        query = '''
            SELECT year, month, day, counts 
            FROM {s} 
            WHERE table_name = '{t}'
            ORDER BY 1 DESC, 2 DESC, 3 DESC
        '''.format(t=table_name, s=storage_table)
        results = run_redshift_query(query, db_config, fetch=True)
        df = pd.DataFrame(results)
        df.columns = ['year', 'month', 'day', 'counts']
    else:
        output_location = update_red(athena_q)
        bucket = output_location.split('/')[2]
        key = '/'.join(output_location.split('/')[3:])
        s3 = boto3.client('s3')
        df = pd.read_csv(s3.get_object(Bucket=bucket, Key=key)['Body'], header=0)
        df.columns = ['year', 'month', 'day', 'table_name', 'counts']

    min_c, max_c = min(df.counts), max(df.counts)
    df_ = df[(df.year == y) & (df.month == m) & (df.day == d)].reset_index()

    # Assertions
    print('Asserting Now!!')
    assert df_.year[0] == y, 'Year for {} does not match'.format(table_name)
    assert df_.month[0] == m, 'Month for {} does not match'.format(table_name)
    assert df_.day[0] == d, 'Day for {} does not match'.format(table_name)
    # assert df_.counts[0] / min_c > 0.85, 'Counts are 15% lesser than minimum for {}'.format(table_name)
    print('Asserted!!')
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Asserting Tables for Provided Date')

    parser.add_argument('--tables', action='store', type=str, required=True, help='Tables to be Asseted. Multiple tables separate by PIPE.')
    parser.add_argument('--execution_date', action='store', type=str, required=True, help='Execution Date')
    parser.add_argument('--store_flag', action='store', type=int, default=1, help='Whether to store checksums?')
    parser.add_argument('--db_conn', action='store', type=str, default='TRENDZ_REDSHIFT_PSYCOPG2',
                        required=False, help='Storage Database')
    parser.add_argument('--storage_table', action='store', type=str, default='athena_checksums.daily_counts',
                        required=False, help='Table to store checksum data to')
    args = parser.parse_args()

    dt = datetime.strptime(args.execution_date[:10], '%Y-%m-%d')
    y, m, d = dt.year, dt.month, dt.day

    config = json.load(open('/home/ubuntu/1mg/airflow/configs/config.json', 'r'))
    access_key, secret_key = config['S3']['aws_access_key_id'], config['S3']['aws_secret_access_key']
    db_config = config[args.db_conn]

    tables = args.tables.split('|')
    for t in tables:
        check_red(t, y, m, d, access_key, secret_key, db_config, args.storage_table, args.store_flag)
