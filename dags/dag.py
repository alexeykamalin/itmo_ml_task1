from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from data.make_dataset import load_data, prepare_data
from models.predict_model import test_model
from models.train_model import train_model



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'iris_classification_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    prepare_data_task = PythonOperator(
        task_id='prepare_data',
        python_callable=prepare_data,
    )

    train_model_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )

    test_model_task = PythonOperator(
        task_id='test_model',
        python_callable=test_model,
    )

    load_data_task >> prepare_data_task >> train_model_task >> test_model_task