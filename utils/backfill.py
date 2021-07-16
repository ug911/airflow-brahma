import argparse
from subprocess import Popen, PIPE
from datetime import datetime, timedelta

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag_name", help="Dag Name", type=str)
    parser.add_argument("--start_date", '-s', help="Enter start date in format: YYYY-MM-DD", type=str)
    parser.add_argument("--end_date", '-e', help="Enter end date in format: YYYY-MM-DD", type=str)
    args = parser.parse_args()

    dag_name = args.dag_name
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

    if end_date <= start_date:
        raise Exception('End Date is before or Same as the Start Date')

    next_date = start_date + timedelta(days=1)
    while next_date > end_date:
        start_date += timedelta(days=1)
        sdt = datetime.strftime(start_date, '%Y-%m-%d')
        ndt = datetime.strftime(next_date, '%Y-%m-%d')
        airflow_cmd = 'airflow backfill {} -s {} -e {}'.format(dag_name, sdt, ndt)
        print(airflow_cmd)
        p = Popen(airflow_cmd, shell=True, stdout=PIPE)
        p.wait()
        next_date += timedelta(days=1)
