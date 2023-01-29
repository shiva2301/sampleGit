from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator 


def print_helloworld(ds):
    print(f'execution Date : {ds}')
    return 'hello world, this is my first airflow DAG.'

default_args={
    'owner' : 'me',
    'start_date':datetime(2023,1,1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : True
}

dag=DAG(dag_id='sample-hello_world', description='first sample airflow dag',
    schedule_interval='0 12 * * *',
    start_date=datetime(2023, 1, 1), catchup=True)

hello_operator =PythonOperator(task_id='hello_task',python_callable=print_helloworld,dag=dag)

hello_bash= BashOperator(task_id='hello_bash', bash_command='echo "hello, how are you-{{execution_date}}"',dag=dag)

hello_bash >> hello_operator