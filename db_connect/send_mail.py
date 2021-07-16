#!/usr/bin/env python3
import argparse
from subprocess import Popen, PIPE

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Python Module to Send Emails with Attachment')
    parser.add_argument('--execution_date', action='store', type=str, required=True, help='Execution Date')
    parser.add_argument('--email_to', action='store', type=str, required=True, help='Email To')
    parser.add_argument('--email_subject', action='store', type=str, required=True, help='Email Subject')
    parser.add_argument('--email_body_filename', action='store', type=str, default=None, help='Email Body')
    parser.add_argument('--dt_subject', action='store', type=int, default=0, help='Add Date in Subject')
    parser.add_argument('--attachment_file', action='store', type=str, default=None, help='Download Filename')
    parser.add_argument('--attachment_file_directory', action='store', type=str, default='data', help='Download Directory')
    args = parser.parse_args()

    BASE_DIR = '/home/ubuntu/1mg/airflow'

    try:
        edt = args.execution_date[:10]
    except Exception as e:
        edt = 'No Date'
        print(e)

    email_subject = args.email_subject.format(dt=edt) if args.dt_subject != 0 else args.email_subject

    command = '''mutt {email_to} -s "{email_subject}" '''.format(email_to=args.email_to, email_subject=email_subject)

    if args.attachment_file:
        DATA_DIR = '{}/{}'.format(BASE_DIR, args.attachment_file_directory)
        command += '''-a {}/{} '''.format(DATA_DIR, args.attachment_file)

    if args.email_body_filename:
        command += '''< {}/{}'''.format(BASE_DIR, args.email_body_filename)

    print(command)
    p = Popen(command, shell=True, stdout=PIPE)
    p.wait()
