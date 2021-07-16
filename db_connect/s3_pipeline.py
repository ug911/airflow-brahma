"""
--> S3Pipeline
"""
import re
import json
from subprocess import Popen
from system_pipeline import SystemPipeline


class S3Pipeline:
    """
    S3 pipeline is a class that stores all the S3 related functions
    """
    @staticmethod
    def drop_s3_file(s3_location):
        """
        Drop file(s) on an S3 location
        Args:
            s3_location ():

        Returns:

        """
        command = "sudo s3cmd del -r {s3_location}".format(s3_location=s3_location)
        print(command)
        SystemPipeline.run_command(command)

    @staticmethod
    def put_to_s3(local_file_path, s3_path):
        """
        Move file(s) from local system to an S3 location
        Args:
            local_file_path ():
            s3_path ():

        Returns:

        """
        command = "sudo s3cmd put {local_file_path} {s3_path}" \
            .format(local_file_path=local_file_path, s3_path=s3_path)
        print(command)
        process = Popen(command, shell=True)
        process.wait()

    @staticmethod
    def mv_s3_to_s3(actual_location, move_location, non_recursive=False):
        """
        Move file(s) from one S3 location to another
        Args:
            actual_location ():
            move_location ():

        Returns:

        """
        if not non_recursive:
            command = "s3cmd mv --recursive {actual_location} {move_location}" \
            .format(actual_location=actual_location, move_location=move_location)
        else:
            command = "s3cmd mv {actual_location} {move_location}" \
                .format(actual_location=actual_location, move_location=move_location)
        print(command)
        SystemPipeline.run_command(command)

    @staticmethod
    def cp_s3_to_s3(actual_location, move_location):
        """
        Copy file(s) from one S3 location to another
        Args:
            actual_location ():
            move_location ():

        Returns:

        """
        command = "s3cmd sync --recursive {actual_location} {move_location}" \
            .format(actual_location=actual_location, move_location=move_location)
        print(command)
        SystemPipeline.run_command(command)

    @staticmethod
    def get_from_s3(s3_path, local_file_path):
        """
        Download a file from an S3 location to your local system
        Args:
            s3_path ():
            local_file_path ():

        Returns:

        """
        command = "sudo s3cmd get {s3_path} {local_file_path}" \
            .format(s3_path=s3_path, local_file_path=local_file_path)
        print(command)
        SystemPipeline.run_command(command)

    @staticmethod
    def check_s3_location_path(s3_path):
        """
        Check whether the structure of S3 is valid or not
        Args:
            s3_path ():

        Returns:

        """
        path_wo_suffix = re.sub('^s3://', '', s3_path)
        split_path = [x for x in path_wo_suffix.split('/') if x != '']
        if len(split_path) < 3:
            raise Exception('Trying to delete a Bucket or Folder in a Bucket')

    @staticmethod
    def move_delete(path_category_to_delete, path_to_delete):
        """
        1. Delete S3 temp location
        2. Move S3 location to a temp location
        Args:
            path_category_to_delete ():
            path_to_delete ():

        Returns:

        """
        print('Move - Delete with {} {}'.format(path_category_to_delete, path_to_delete))
        temp_actual_config = json.load\
            (open('/home/ubuntu/1mg/airflow/configs/temp_actual_move.config.json'))

        move_config = temp_actual_config[path_category_to_delete]
        temp_location = move_config['temp_location'].format(path_to_delete)
        actual_location = move_config['actual_location'].format(path_to_delete)

        # S3Pipeline.check_s3_location_path(temp_location)
        # S3Pipeline.check_s3_location_path(actual_location)
        S3Pipeline.drop_s3_file(actual_location)
        # S3Pipeline.mv_s3_to_s3(actual_location, temp_location)

        print('Done Move - Delete')
        return 1
