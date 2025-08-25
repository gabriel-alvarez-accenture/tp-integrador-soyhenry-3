from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import csv
import boto3
from dotenv import load_dotenv
import os


dag_folder = os.path.dirname(__file__)
env_path = os.path.join(dag_folder, ".env")
load_dotenv(dotenv_path=env_path)

FILE_PATH               = os.getenv("FILE_PATH")
aws_access_key_id       = os.getenv("aws_access_key_id")
aws_secret_access_key   = os.getenv("aws_secret_access_key")



def load_file_aws():
    def flatten_entry(entry):
        flat = {}
        for key, value in entry.items():
            if isinstance(value, dict):
                for subkey, subvalue in value.items():
                    flat[f"{key}_{subkey}"] = subvalue
            elif isinstance(value, list) and key == "weather":
                for subkey, subvalue in value[0].items():
                    flat[f"{key}_{subkey}"] = subvalue
            else:
                flat[key] = value
        return flat
    
    def upload_to_s3():
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name='us-east-2'
        )

        s3.upload_file(
            Filename=f"{FILE_PATH}.csv",
            Bucket='bucket-s3-tp3-soyhenry',
            Key='raw/Patagonia_-41_810147_-68_906269_1722470400_1754092799_689025ec11032e000839b7c9.csv'
        )


    with open(f"{FILE_PATH}.json", 'r', encoding='utf-8') as f:
        data = json.load(f)

    flattened_data = [flatten_entry(entry) for entry in data]

    all_keys = set()
    for row in flattened_data:
        all_keys.update(row.keys())

    with open(f"{FILE_PATH}.csv", 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=sorted(all_keys))
        writer.writeheader()
        writer.writerows(flattened_data)
    
    upload_to_s3()

def execute_step_function():
    sfn = boto3.client(
        'stepfunctions',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='us-east-2'
    )

    # ARN de la Step Function que querés ejecutar
    state_machine_arn = 'arn:aws:states:us-east-2:106220368786:stateMachine:PROCESO_ETL_TP3_SOYHENRY'

    # Ejecutar la Step Function
    sfn.start_execution(
        stateMachineArn=state_machine_arn
    )


with DAG(
    dag_id="dag_AWS_dataset", 
    start_date=datetime(2025, 1, 1),
    schedule_interval=None, 
    catchup=False,
    tags=["soyhenry"],
) as dag:

    task_load_file_aws = PythonOperator(
        task_id="task_load_file_aws",
        python_callable=load_file_aws,
    )

    task_execute_step_function = PythonOperator(
        task_id="task_execute_step_function",
        python_callable=execute_step_function,
    )

    task_load_file_aws >> task_execute_step_function