from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import boto3
from dotenv import load_dotenv
import os
import time


dag_folder = os.path.dirname(__file__)
env_path = os.path.join(dag_folder, ".env")
load_dotenv(dotenv_path=env_path)

FILE_PATH               = os.getenv("FILE_PATH")
aws_access_key_id       = os.getenv("aws_access_key_id")
aws_secret_access_key   = os.getenv("aws_secret_access_key")




def execute_step_function():
    time.sleep(180)  
    
    sfn = boto3.client(
        'stepfunctions',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='us-east-2'
    )

    # ARN de la Step Function que querés ejecutar
    state_machine_arn = 'arn:aws:states:us-east-2:106220368786:stateMachine:PROCESO_ETL_API_POKEMON_TP3_SOYHENRY'

    # Ejecutar la Step Function
    sfn.start_execution(
        stateMachineArn=state_machine_arn
    )


with DAG(
    dag_id="dag_AWS_api_pokemon", 
    start_date=datetime(2025, 1, 1),
    schedule_interval=None, 
    catchup=False,
    tags=["soyhenry"],
) as dag:
    
    task_extraction_airbyte = BashOperator(
        task_id='extraction_airbyte',
        bash_command='docker exec airbyte-container python /app/airbyte.py'
    )

    task_execute_step_function = PythonOperator(
        task_id="task_execute_step_function",
        python_callable=execute_step_function,
    )

    task_extraction_airbyte >> task_execute_step_function