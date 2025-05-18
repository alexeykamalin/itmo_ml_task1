from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

def load_data():
    iris = load_iris()
    df = pd.DataFrame(data=iris.data, columns=iris.feature_names)
    df['target'] = iris.target
    return df

def preprocess_data(ti):
    df = ti.xcom_pull(task_ids='load_data')
    # Масштабирование признаков
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df.drop('target', axis=1))
    df_scaled = pd.DataFrame(X_scaled, columns=df.columns[:-1])
    df_scaled['target'] = df['target']
    return df_scaled

def split_data(ti):
    df = ti.xcom_pull(task_ids='preprocess_data')
    X_train, X_test, y_train, y_test = train_test_split(
        df.drop('target', axis=1),
        df['target'],
        test_size=0.2,
        random_state=42
    )
    return {'X_train': X_train, 'X_test': X_test, 'y_train': y_train, 'y_test': y_test}

def train_model(ti):
    data = ti.xcom_pull(task_ids='split_data')
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(data['X_train'], data['y_train'])
    return clf

def evaluate_model(ti):
    data = ti.xcom_pull(task_ids='split_data')
    model = ti.xcom_pull(task_ids='train_model')
    y_pred = model.predict(data['X_test'])
    accuracy = accuracy_score(data['y_test'], y_pred)
    print(f"Model accuracy: {accuracy:.2f}")
    return accuracy

with DAG(
    'iris_classification',
    default_args=default_args,
    description='A simple Iris classifier DAG',
    schedule_interval='@daily',
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    preprocess_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data
    )

    split_task = PythonOperator(
        task_id='split_data',
        python_callable=split_data
    )

    train_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model
    )

    evaluate_task = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model
    )

    load_task >> preprocess_task >> split_task >> train_task >> evaluate_task