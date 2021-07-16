"""
--> SystemPipeline
"""
from subprocess import Popen, PIPE


class SystemPipeline:
    """
    System pipeline is a class that stores all the system related functions
    """
    @staticmethod
    def run_command(command):
        """
        Run a command on command line
        Args:
            command ():

        Returns:

        """
        process = Popen(command, shell=True)
        process.wait()

    @staticmethod
    def run_command_get_op(command):
        """
        1. Run a command on command line and return the output
        Args:
            command ():

        Returns:

        """
        output = Popen(command, shell=True, stdout=PIPE).stdout
        output = output.read().decode("utf-8")
        return output

    @staticmethod
    def del_from_local(csv_file_path):
        """
        Delete a file from the local system
        Args:
            csv_file_path ():

        Returns:

        """
        delete_command = "sudo rm {}".format(csv_file_path)
        SystemPipeline.run_command(delete_command)
