import boto3
import pdb
import json
import argparse
import requests
from time import time, sleep
from pprint import pprint

def send_flock_ping(message, default_flock_incoming_url, flock_incoming_url, message_sent):
    headers = {
        'Content-Type': 'application/json'
    }
    flock_payload = {"text": message}
    response = requests.request("POST", default_flock_incoming_url, headers=headers, data=json.dumps(flock_payload))
    print(response.text.encode('utf8'))
    if default_flock_incoming_url != flock_incoming_url and not message_sent:
        message_sent = True
        response = requests.request("POST", flock_incoming_url, headers=headers, data=json.dumps(flock_payload))
        print(response.text.encode('utf8'))
    return message_sent

def resume_dms_task(job_name, task_arn):
    success = False
    if task_arn:
        try:
            response = client.start_replication_task(
                ReplicationTaskArn='arn:aws:dms:ap-south-1:831059512818:task:N7N3LT2XFN6UTXPXR24D3JTU5A',
                StartReplicationTaskType='resume-processing',
            )
            print(response)
            success = True
        except Exception as e:
            print(e)
    else:
        print('Trying to restart {} but no Task ARN found.'.format(job_name))
    return success

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Python Module to Check if DMS Jobs have been successful')
    parser.add_argument('--execution_date', action='store', type=str, required=True, help='Execution Date')
    parser.add_argument('--debug', action='store', type=int, required=False, default=0, help='Debug Flag')
    parser.add_argument('--flock_channel', action='store', type=str, required=False,
                        default='Data Engineering Team', help='Name of Flock Channel to Post in case of Delay')
    parser.add_argument('--default_flock_channel', action='store', type=str, required=False,
                        default='Data Engineering Team', help='Name of Default Flock Channel')
    parser.add_argument('--message', action='store', type=str, required=False, default='Default Message',
                        help='Message to Send')
    parser.add_argument('--trigger_failure', action='store', type=int, required=False, default=0,
                        help='Trigger Failure')
    args = parser.parse_args()

    client = boto3.client('dms')

    flock_config = json.load(open('/home/ubuntu/1mg/airflow/configs/flock_config.json', 'r'))
    incoming_webhook_id = flock_config.get(args.flock_channel, {}).get('incoming_webhook', None)
    default_incoming_webhook_id = flock_config.get(args.default_flock_channel, {}).get('incoming_webhook', None)
    message = args.message
    if not incoming_webhook_id:
        raise Exception('Can not find Incoming Webhook')

    flock_incoming_url = "https://api.flock.com/hooks/sendMessage/{}".format(incoming_webhook_id)
    default_flock_incoming_url = "https://api.flock.com/hooks/sendMessage/{}".format(default_incoming_webhook_id)


    headers = {
        'Content-Type': 'application/json'
    }

    replication_tasks_success = False
    start = time()
    counter = 1
    message_sent = False
    while not replication_tasks_success:
        try:
            response = client.describe_replication_tasks(
                Filters=[
                    {
                        'Name': 'replication-task-arn',
                        'Values': [
                            'arn:aws:dms:ap-south-1:831059512818:task:2S2DWDDQVPVNQZASHPSMJXMT3Q',
                            'arn:aws:dms:ap-south-1:831059512818:task:AW6CH3UJHZ2ZLFS5ML32KHIHSA',
                            'arn:aws:dms:ap-south-1:831059512818:task:HQMFMJQLDU66YNZANDO6A6KZHU',
                            'arn:aws:dms:ap-south-1:831059512818:task:JOX2UO62BGYFS25SUV5NNFWGD4',
                            'arn:aws:dms:ap-south-1:831059512818:task:HXKV67OJD737FT7NEWPPCN73QQ',
                            'arn:aws:dms:ap-south-1:831059512818:task:6FB4OK36GYWVWY4RVZUF3FCTSQ',
                            'arn:aws:dms:ap-south-1:831059512818:task:N7N3LT2XFN6UTXPXR24D3JTU5A',
                            'arn:aws:dms:ap-south-1:831059512818:task:V5PXNYQA4BV6I36KV4PBMTIJDQ',
                            'arn:aws:dms:ap-south-1:831059512818:task:OI5Y3ZNTUZZCF2WBEDXE7JRSWJ6IZT4PQGIILJI'
                        ]
                    },
                ]
            )
            if args.debug:
                pprint(response)
        except Exception as e:
            print('Error while fetching DMS replication tasks -> {}'.format(e))
            response = {}
        replication_tasks = response.get('ReplicationTasks') or []
        print('-- Run {} Started --------------------------------------------'.format(counter))
        if replication_tasks:
            task_success_flag = True
            failed_tasks = []
            for _r in replication_tasks:
                status = _r.get('Status') or 'missing'
                tables_loading_flag = _r.get('ReplicationTaskStats', {}).get('TablesLoading', -1)
                task_arn = _r.get('ReplicationTaskArn') or None
                job_name = _r.get('ReplicationTaskIdentifier') or 'dummy'
                print('{} has Status ({}) and TablesLoading Flag = {}'.format(job_name, status, tables_loading_flag))
                if args.debug: 
                    pdb.set_trace()
                if status != 'running' or tables_loading_flag != 0:
                    task_success_flag = False
                    if status != 'running':
                        failed_tasks.append((job_name, task_arn))
                    break
            if task_success_flag and args.trigger_failure == 0:
                replication_tasks_success = True
        counter += 1
        if not replication_tasks_success:
            print('Replication is not successful after {} secs'.format(round(time() - start)))
            if len(failed_tasks) > 0:
                for _task in failed_tasks:
                    flock_msg = "{} has failed. Resuming the task. {}".format(job_name, message)
                    send_flock_ping(flock_msg, default_flock_incoming_url, flock_incoming_url, True)
                    success = resume_dms_task(job_name, task_arn)
                    flock_msg = "{} has failed. Resuming the task has status success = {}, {}".format(job_name, success,
                                                                                                      message)
                    send_flock_ping(flock_msg, default_flock_incoming_url, flock_incoming_url, True)
                    if not success:
                        raise Exception('Unable to restart DMS Task')
            sleep(10)

        if counter%6 == 0:
            flock_msg = "Delay due to DMS. Time Elapsed : {}. Message: {}".format(counter * 10, message)
            message_sent = send_flock_ping(flock_msg, default_flock_incoming_url, flock_incoming_url, message_sent)

    print('Replication is found successful after {} secs'.format(round(time() - start)))
