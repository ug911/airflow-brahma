"""
--> VersionPipeline
"""
from datetime import datetime


class VersionPipeline:
    """
    Version pipeline is a class that stores all the version creation functions
    """
    @staticmethod
    def create_date_version(execution_date):
        """
        Create a date version based on the execution date
        Args:
            execution_date ():

        Returns:

        """
        try:
            edt = datetime.strptime(execution_date[:10], '%Y-%m-%d')
            hour = execution_date[11:13]
            minute = execution_date[14:16]
        except Exception as exp:
            print(exp)
            try:
                edt = datetime.strptime(execution_date, '%Y-%m-%d %H:%M:%S')
                hour, minute = '{:02d}'.format(edt.hour), '{:02d}'.format(edt.minute)
            except Exception as exp:
                print(exp)
                edt = datetime.strptime(execution_date.split('T')[0], '%Y-%m-%d')
                hour, minute = '{:02d}'.format(0), '{:02d}'.format(0)
        year, month, day = edt.year, '{:02d}'.format(edt.month), '{:02d}'.format(edt.day)
        return year, month, day, hour, minute
