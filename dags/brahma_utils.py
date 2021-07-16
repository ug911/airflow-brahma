import constants as Constants
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
SLACK_CONN_ID = 'slack'


def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    print(context)
    slack_msg = """
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=str(context.get('task_instance').log_url).replace('localhost', Constants.HOST),
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',)
    return failed_alert.execute(context=context)


def make_dag(dag_config, default_args):
    '''
    Builds the dag from the provided configuration
    :param dag_config: Stage wise dag configuration
    :param default_args: Default Arguments for the DAG
    :return: Configured DAG
    '''
    # Adds specific parameters to the dag
    default_args[Constants.DAG_OWNER] = dag_config[Constants.DAG_ARGS][Constants.DAG_OWNER]
    default_args[Constants.DAG_EMAIL] = dag_config[Constants.DAG_ARGS][Constants.DAG_EMAIL]
    default_args[Constants.START_DATE] = datetime.now() - timedelta(days=10)
    default_args[Constants.RETRY_DELAY] = timedelta(minutes=3)
    default_args['on_failure_callback'] = task_fail_slack_alert
    default_args[Constants.RETRIES] = dag_config[Constants.DAG_ARGS][Constants.RETRIES] \
        if Constants.RETRIES in dag_config[Constants.DAG_ARGS] else default_args[Constants.RETRIES]

    # Initialises the DAG
    dag = DAG(dag_config[Constants.DAG_ARGS][Constants.DAG_NAME],
              schedule_interval=dag_config[Constants.DAG_ARGS][Constants.SCHEDULE_INTERVAL],
              default_args=default_args,
              catchup=False
              )

    startstage = BashOperator(task_id='start', bash_command=Constants.DEFAULT_BASH_PROCESS, dag=dag)
    attach_start = False
    for i in range(dag_config[Constants.DAG_ARGS][Constants.DAG_NUM_STAGES]):
        stage = 's' + str(i + 1)
        endstage = BashOperator(task_id='end_' + stage, bash_command=Constants.DEFAULT_BASH_PROCESS, dag=dag)
        for process in dag_config[stage].keys():
            decipher_command(dag, dag_config[stage][process], startstage, endstage, attach_start)
        attach_start = True
        startstage = endstage

    return dag


def get_process_config(process_config):
    '''
    Make the Bash Commands for all the tasks
    :param process_config: Configuration for a single process
    :return: Zip with task IDs and bash commands to run
    '''
    home = process_config[Constants.HOME_DIR]
    prefix = process_config[Constants.PREFIX]
    task_ids = process_config[Constants.TASK_IDS].split(Constants.SPLIT_BY)
    attach_exec_dt = [x for x in task_ids
                      if not x in process_config.get(Constants.NO_EXEC_DT_TASKS, '').split(Constants.SPLIT_BY)]
    run_with_sudo = 'sudo ' if not process_config.get(Constants.NO_SUDO) else ''
    bash_cmds, final_tasks = [], []
    for task in task_ids:
        cmd = process_config.get(Constants.BASH_CMDS, {}).get(task, '')
        if cmd != '':
            exec_dt_string = Constants.EXEC_DT if task in attach_exec_dt else ''
            bash_cmds.append('{}{} {}{} {}'.format(run_with_sudo, prefix, home, cmd, exec_dt_string))
            final_tasks.append(task)
    tasks_to_run = zip(final_tasks, bash_cmds)
    return tasks_to_run


def is_parallel_config(process_config):
    is_parallel = process_config.get(Constants.IS_PARALLEL, False)
    return is_parallel


def decipher_command(dag, process_config, start_stage, end_stage, attach_start):
    '''
    Make the DAG for a Single Stage
    :param dag: DAG Object
    :param process_config: Configuration of the Stage Process
    :param start_stage: Stage's Start Task
    :param end_stage: Stage's End Task
    :param attach_start: Does the Start Task need to be attached to the First Process?
    '''
    tasks_to_run = get_process_config(process_config)
    is_parallel = is_parallel_config(process_config)
    task_counter = 0
    for task, cmd in tasks_to_run:
        process = BashOperator(task_id=task, bash_command=cmd, dag=dag)
        if not is_parallel:
            if attach_start and task_counter == 0:
                process.set_upstream(start_stage)
            elif task_counter > 0:
                process.set_upstream(last_process)
            task_counter += 1
            last_process = process
        else:
            if attach_start:
                process.set_upstream(start_stage)
            end_stage.set_upstream(process)
    if not is_parallel and task_counter != 0:
        end_stage.set_upstream(process)
    return 1

