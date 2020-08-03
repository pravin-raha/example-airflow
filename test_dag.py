from datetime import timedelta

from airflow import DAG

from pprint import pprint

from airflow.operators.python_operator import PythonOperator

from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

import mlflow

default_args = {
    'owner': 'pravin',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# [START instantiate_dag]
dag = DAG(
    'test_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=None,
)
# [END instantiate_dag]

# t1 = BashOperator(
#     task_id='git_clone',
#     bash_command='git clone -b $branch $url',
#     env={'url': '{{ dag_run.conf["git_url"] if dag_run else "" }}',
#          'branch' : '{{ dag_run.conf["git_branch_name"] if dag_run else "master" }}'
#         },
#     dag=dag,
# )


# t2 = BashOperator(
#     task_id='pwd',
#     bash_command='ls',
#     dag=dag,
# )

def run_ml(**context):
    config_params = {"alpha": context['dag_run'].conf['alpha'] }
    run_obj = mlflow.projects.run(
        context['dag_run'].conf['git_url'],
        version=context['dag_run'].conf['git_branch_name'],
        parameters=config_params,
        experiment_name="workflow"
        )
    return run_obj.run_id


t3 = PythonOperator(
    task_id='p_mlflow_run',
    python_callable=run_ml,
    provide_context=True,
    dag=dag,
)


# def print_context(ds, **kwargs):
#     pprint(kwargs)
#     print(ds)
#     return 'Whatever you return gets printed in the logs'


# run_this = PythonOperator(
#     task_id='print_the_context',
#     provide_context=True,
#     python_callable=print_context,
#     dag=dag,
# )

# t1 >> t2 >> 

t3
# run_this
