"""
--> AthenaPipeline
"""
import boto3
from uuid import UUID
import time
import re
from datetime import datetime, timedelta

from retry import retry
from version_pipeline import VersionPipeline
from s3_pipeline import S3Pipeline


class AthenaPipeline:
    """
    Athena pipeline is a class that stores all the athena database related functions
    """
    @staticmethod
    def version_uuid(uuid):
        """
        Check the format of execution id
        Args:
            uuid ():

        """
        try:
            return UUID(uuid).version
        except ValueError:
            return None

    @staticmethod
    def get_query_results(exec_id):
        """
        Get S3 output location of your query results
        Args:
            exec_id ():
        """
        athena = boto3.client('athena')
        response = athena.get_query_execution(QueryExecutionId=exec_id)
        output_location = response.get('QueryExecution', {}).get('ResultConfiguration', {})\
            .get('OutputLocation', None)
        if not output_location:
            raise Exception('File not present at {}'.format(output_location))
        return output_location

    @staticmethod
    @retry(tries=3, delay=120)
    def run_query(query, athena_schema='dim_fact',
                  s3_path='s3://temp-encryption-logs/athena_connect/results/'):
        """
        Run a query on athena.
        Args:
            query ():
            athena_schema ():
            s3_path ():

        """
        print(query)
        client = boto3.client('athena')
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': athena_schema},
            ResultConfiguration={'OutputLocation': s3_path}
        )
        print('Execution ID: ' + response['QueryExecutionId'])
        execution_id = response.get('QueryExecutionId', None)
        status = AthenaPipeline.wait(execution_id)
        if status.lower() in ['failed', 'failure', 'fails']:
            raise Exception("Query failed - Retrying the task for 3 times with an "
                            "interval of 2 minutes")
        return execution_id

    @staticmethod
    def get_exec_status(exec_id):
        """
        Get execution status of Athena query
        Args:
            exec_id ():

        """
        client = boto3.client('athena')
        status = client.get_query_execution(QueryExecutionId=exec_id).get('QueryExecution', {})\
            .get('Status', {}).get(
                'State', None)
        return status

    @staticmethod
    def check_object(bucket, key):
        """
        Check if the output S3 bucket exists or not
        Args:
            bucket ():
            key ():

        """
        print('checking {}, {}'.format(bucket, key))
        s3_obj = boto3.resource('s3')
        _bucket = s3_obj.Bucket(bucket)
        obj = list(_bucket.objects.filter(Prefix=key))
        if len(obj) > 0:
            found = True
        else:
            found = False
        print('found: {}'.format(found))
        return found

    @staticmethod
    def wait(exec_id, sleep_time=10, raise_on_fail=True):
        """
        Wait till the query execution is completed
        Args:
            exec_id ():
            sleep_time ():
            raise_on_fail ():

        """
        start = time.time()

        if not AthenaPipeline.version_uuid(exec_id):
            message = "Error while checking the Execution ID Format. " \
                      "Please check the Execution ID = {}".format(exec_id)
            raise Exception(message)

        status, success = '', False
        while not success and exec_id:
            status = AthenaPipeline.get_exec_status(exec_id)
            print('The Execution ID for the query is {} and the status is {} in {}secs'
                  .format(exec_id, status, round(time.time() - start)))
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                success = True
                break
            time.sleep(sleep_time)

        if not success:
            raise Exception('Possible Error in Query')

        if status != 'SUCCEEDED' and raise_on_fail:
            raise Exception('Query Execution Failed:{}'.format(exec_id))

        print('Done!!! The Execution ID for the query is {} and the status is {} in {}secs'
              .format(exec_id, status, round(time.time() - start)))

        return status

    @staticmethod
    def replace_ymd_by_regex(query, eval_year, eval_month, eval_day):
        """
        Replace execution year, month, day by the ones added in arguments
        Args:
            query ():
            eval_year ():
            eval_month ():
            eval_day ():

        """
        _query = re.sub('{eval_year}', str(eval_year), query)
        _query = re.sub('{eval_month}', str(eval_month), _query)
        _query = re.sub('{eval_day}', str(eval_day), _query)
        _query = re.sub('{eval_month:02d}', '{:02d}'.format(int(eval_month)), _query)
        _query = re.sub('{eval_day:02d}', '{:02d}'.format(int(eval_day)), _query)
        return _query

    @staticmethod
    def drop_table(athena_table, execution_date=''):
        """
        Drop table on Athena
        Args:
            execution_date ():
            athena_table ():

        """
        if execution_date:
            execution_date = execution_date.split('T')[0].split('-')
            athena_table = re.sub('{year}', execution_date[0], athena_table)
            athena_table = re.sub('{month}', execution_date[1], athena_table)
            athena_table = re.sub('{day}', execution_date[2], athena_table)
        print('Dropping table = {}'.format(athena_table))
        query = "DROP TABLE IF EXISTS {athena_table}"\
            .format(athena_table=athena_table)
        AthenaPipeline.run_query(query)
        print('Done - Dropping table')

    @staticmethod
    def athena_query(query_filename, ymd, execution_date, bucket='', input_key='',
                     replace_ymd_by_regex=0, date_counter=-1):
        """
        Run a query on Athena based on different arguments
        Args:
            query_filename ():
            bucket ():
            input_key ():
            ymd ():
            execution_date ():
            replace_ymd_by_regex ():
            date_counter ():

        """
        query = open('/home/ubuntu/1mg/airflow/tasks/queries/' +
                     query_filename, 'r').read()
        key, key_ints = '', ''
        if date_counter is not None and date_counter >= 0:
            execution_date = (datetime.now() - timedelta(days=int(date_counter))) \
                .strftime('%Y-%m-%d %H:%M:%S')
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

        if ymd != 0:
            key = input_key.format(_year, _month, _day)
            key_ints = input_key.format(_year, str(edt.month), str(edt.day))
            eval_year, eval_month, eval_day = edt.year, edt.month, edt.day
            if replace_ymd_by_regex != 0:
                query = AthenaPipeline.replace_ymd_by_regex(query=query,
                                                            eval_year=eval_year,
                                                            eval_month=eval_month,
                                                            eval_day=eval_day)
            else:
                query = query.format(eval_year=eval_year, eval_month=eval_month,
                                     eval_day=eval_day)

        found = AthenaPipeline.check_object(bucket, key) or \
                AthenaPipeline.check_object(bucket, key_ints) \
            if bucket != '' and key != '' and ymd != 0 else False
        if not found:
            execution_id = AthenaPipeline.run_query(query)
            return execution_id

    @staticmethod
    def athena_dq(query_filename, athena_table, execution_date):
        """
        1. Drop table on Athena
        2. Run a versioned query on Athena.
        Args:
            query_filename ():
            athena_table ():
            execution_date ():

        """
        query = open('/home/ubuntu/1mg/airflow/tasks/queries/' +
                     query_filename, 'r').read()

        # Date version
        year, month, day, hour, minute = \
            VersionPipeline.create_date_version(execution_date)
        version = '{}_{}_{}_{}_{}'.format(year, month, day, hour, minute)

        AthenaPipeline.drop_table(athena_table)
        query = re.sub('{version}', version, query)
        AthenaPipeline.run_query(query)

    @staticmethod
    def athena_mdq(query_filename, athena_table, path_category_to_delete,
                   path_to_delete):
        """
        1. Drop temp location and move current S3 location to a temp location.
        2. Drop table on Athena
        3. Run a query on Athena.
        Args:
            query_filename ():
            athena_table ():
            path_category_to_delete ():
            path_to_delete ():

        """
        query = open('/home/ubuntu/1mg/airflow/tasks/queries/' +
                     query_filename, 'r').read()

        S3Pipeline.move_delete(path_category_to_delete, path_to_delete)
        AthenaPipeline.drop_table(athena_table)
        AthenaPipeline.run_query(query)

    @staticmethod
    def athena_download(query_filename, download_directory, download_filename):
        """
        1. Run a query on Athena
        2. Download the output to your local system
        Args:
            query_filename ():
            download_directory ():
            download_filename ():

        """
        query = open('/home/ubuntu/1mg/airflow/tasks/queries/' +
                     query_filename, 'r').read()
        data_dir = "/home/ubuntu/1mg/airflow/{}/{}"\
            .format(download_directory, download_filename)

        execution_id = AthenaPipeline.run_query(query)
        output_location = AthenaPipeline.get_query_results(execution_id)
        S3Pipeline.get_from_s3(output_location, data_dir)

    @staticmethod
    def athena_query_move(query_filename, s3_path, non_recursive=False):
        """
        1. Run a query on Athena
        2. Move the results to a different S3 location
        Args:
            query_filename ():
            s3_path ():
            non_recursive ():
        """
        query = open('/home/ubuntu/1mg/airflow/tasks/queries/' +
                     query_filename, 'r').read()

        execution_id = AthenaPipeline.run_query(query)
        output_location = AthenaPipeline.get_query_results(execution_id)
        S3Pipeline.mv_s3_to_s3(output_location, s3_path, non_recursive)

    @staticmethod
    def athena_repair(athena_table, s3_path, execution_date, additional_partition=''):
        assert s3_path.__contains__('year={year}/month={month}/day={day}/'), \
            'S3 path should contain year-month-day partition.'

        client = boto3.client('s3')
        s3_path = s3_path.replace('s3://', '').split('/', 1)
        bucket, s3_path = s3_path[0], s3_path[1]
        if not s3_path.endswith('/'):
            s3_path += '/'

        ymd = execution_date.split('T')[0].split('-')
        year, month, day = ymd[0], ymd[1], ymd[2]
        s3_path = s3_path.replace('{year}', year).replace('{month}', month) \
            .replace('{day}', day)

        response = client.list_objects(
            Bucket=bucket,
            Delimiter=',',
            Prefix=s3_path
        )

        final_partition_set = set()
        for res in response['Contents']:
            path = res['Key'].rsplit('/', 1)
            final_partition_set.add(path[0])

        for _partition in final_partition_set:
            if additional_partition == 'ymd':
                _partition = 'year={}, month={}, day={}'.format(year, month, day)
            else:
                if additional_partition:
                    additional_partition = additional_partition.replace("|", "'")
                    __partition = 'year={}, month={}, day={}, {}, '.format(year, month, day,
                                                                           additional_partition)
                else:
                    __partition = 'year={}, month={}, day={}, '.format(year, month, day)
                _partition = __partition + _partition.replace(s3_path, '').replace('/', "', ")\
                    .replace('=', "='") + "'"

            if '%2F' in _partition:
                _partition = _partition.replace('%2F', '/')

            query = "ALTER TABLE {} ADD IF NOT EXISTS PARTITION ({})"\
                .format(athena_table, _partition)
            print(_partition, query)
            try:
                AthenaPipeline.run_query(query)
            except:
                pass