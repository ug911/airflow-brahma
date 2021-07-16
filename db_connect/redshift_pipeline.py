"""
--> RedshiftPipeline
"""
import os
import psycopg2
from db_config import REDSHIFT_CONNECTION_STRING, IAM_ROLE


class RedshiftPipeline:
    """
    Redshift pipeline is a class that stores all the redshift database related functions
    """
    @staticmethod
    def redshift_con():
        """
        Create a connection to the redshift database.
        """
        redshift_connect = psycopg2.connect(REDSHIFT_CONNECTION_STRING)
        redshift_cursor = redshift_connect.cursor()
        return redshift_cursor

    @staticmethod
    def truncate_table(redshift_schema, redshift_table):
        """
        Truncate table on redshift
        Args:
            redshift_schema ():
            redshift_table ():
        """
        truncate_command = "TRUNCATE {redshift_schema}.{redshift_table};"\
            .format(redshift_schema=redshift_schema, redshift_table=redshift_table)
        print(truncate_command)
        cur = RedshiftPipeline.redshift_con()
        cur.execute(truncate_command)
        cur.execute("commit")
        cur.close()

    @staticmethod
    def alter_table(redshift_schema, redshift_table, redshift_table_new):
        """
        Alter table on redshift
        Args:
            redshift_schema ():
            redshift_table ():
            redshift_table_new ():
        """
        alter_command = "ALTER TABLE {redshift_schema}.{redshift_table}" \
                        " RENAME TO {redshift_table_new};"\
            .format(redshift_schema=redshift_schema, redshift_table=redshift_table,
                    redshift_table_new=redshift_table_new)
        print(alter_command)
        cur = RedshiftPipeline.redshift_con()
        cur.execute(alter_command)
        cur.execute("commit")
        cur.close()

    @staticmethod
    def unload_table_s3_parquet(redshift_schema, redshift_table, s3_path):
        """
        Unload data from a redshift table to S3 in parquet format
        Args:
            redshift_schema ():
            redshift_table ():
            s3_path ():
        """
        unload_command = "UNLOAD ('SELECT * FROM {redshift_schema}.{redshift_table}') " \
                         "TO '{s3_path}' IAM_ROLE '{iam_role}' FORMAT AS PARQUET;"\
            .format(redshift_schema=redshift_schema, redshift_table=redshift_table,
                    s3_path=s3_path, iam_role=IAM_ROLE)
        print(unload_command)
        cur = RedshiftPipeline.redshift_con()
        cur.execute(unload_command)
        cur.execute("commit")
        cur.close()

    @staticmethod
    def unload_table_s3_csv(redshift_schema, redshift_table, s3_path, col_string):
        """
         Unload data from a redshift table to S3 in csv format.
        Args:
            redshift_schema ():
            redshift_table ():
            s3_path ():
            col_string ():
        """
        unload_command = "UNLOAD ('SELECT {col_string} FROM " \
                         "{redshift_schema}.{redshift_table}') " \
                         "TO '{s3_path}' IAM_ROLE '{iam_role}' FORMAT AS " \
                         "CSV HEADER PARALLEL OFF;"\
            .format(col_string=col_string, redshift_schema=redshift_schema,
                    redshift_table=redshift_table,
                    s3_path=s3_path, iam_role=IAM_ROLE)
        print(unload_command)
        cur = RedshiftPipeline.redshift_con()
        cur.execute(unload_command)
        cur.execute("commit")
        cur.close()

    @staticmethod
    def copy_to_table_parquet(redshift_schema, redshift_table, s3_path):
        """
        Copy data from an S3 location to a redshift table in parquet format
        Args:
            redshift_schema ():
            redshift_table ():
            s3_path ():
        """
        copy_command = "COPY {redshift_schema}.{redshift_table} FROM '{s3_path}' " \
                       "IAM_ROLE '{iam_role}' FORMAT AS PARQUET;"\
            .format(redshift_schema=redshift_schema, redshift_table=redshift_table,
                    s3_path=s3_path, iam_role=IAM_ROLE)
        print(copy_command)
        cur = RedshiftPipeline.redshift_con()
        cur.execute(copy_command)
        cur.execute("commit")
        cur.close()

    @staticmethod
    def copy_s3_redshift_parquet(redshift_schema, redshift_table, s3_path):
        """
        1. Truncate table on redshift
        2. Copy data from an S3 location to a redshift table in parquet format
        Args:
            redshift_schema ():
            redshift_table ():
            s3_path ():
        """
        RedshiftPipeline.truncate_table(redshift_schema, redshift_table)
        RedshiftPipeline.copy_to_table_parquet(redshift_schema, redshift_table, s3_path)
        check_query = "SELECT COUNT(1) FROM {}.{};".format(redshift_schema, redshift_table)
        cur = RedshiftPipeline.redshift_con()
        cur.execute(check_query)
        for _count in cur.fetchall():
            alert_message = "Process = Copy data from S3 into redshift\nStatus = Failed\n" \
                            "Issue = Table is empty, Please check."
            assert _count[0] != 0, alert_message

    @staticmethod
    def alter_redshift_lock(redshift_schema, redshift_table, redshift_new_table):
        """
        1. Remove the lock from redshift table.
        2. Alter table on redshift
        Args:
            redshift_schema ():
            redshift_table ():
            redshift_new_table ():
        """
        query = open('/home/ubuntu/1mg/airflow/tasks/queries/miscellaneous/'
                     'remove_lock_on_tables.sql', 'r').read()\
            .format(table_name=redshift_table)
        print(query)
        cur = RedshiftPipeline.redshift_con()
        cur.execute(query)
        cur.execute("commit")

        # Alter table on Redshift
        RedshiftPipeline.alter_table(redshift_schema, redshift_table, redshift_new_table)

    @staticmethod
    def etl_redshift(file_path):
        """
        Run a query on redshift
        Args:
            file_path ():
        """
        query_file_path = os.path.expanduser('~/1mg/airflow/tasks/queries/') + file_path

        if not query_file_path.__contains__('.sql'):
            query_file_path += '.sql'

        query = open(query_file_path, 'r').read()
        print(query)
        cur = RedshiftPipeline.redshift_con()
        cur.execute(query)
        cur.execute("commit")

    @staticmethod
    def grant_access(file_path):
        """
        Grant access on redshift
        Args:
            file_path ():
        """
        query_file_path = os.path.expanduser('~/1mg/airflow/tasks/queries/') + file_path

        if not query_file_path.__contains__('.sql'):
            query_file_path += '.sql'

        query = open(query_file_path, 'r').read()
        cur = RedshiftPipeline.redshift_con()
        cur.execute(query)
        for grant_command in cur.fetchall():
            grant_command = grant_command[0]
            print(grant_command)
            try:
                cur.execute(grant_command)
                cur.execute("commit")
            except:
                pass
