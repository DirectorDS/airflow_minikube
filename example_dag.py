from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def generate_pascals_triangle(n):
    triangle = []
    for i in range(n):
        row = [1] * (i + 1)
        for j in range(1, i):
            row[j] = triangle[i - 1][j - 1] + triangle[i - 1][j]
        triangle.append(row)
    return triangle

def print_pascals_triangle(**kwargs):
    triangle = generate_pascals_triangle(10)
    print("Треугольник Паскаля:")
    for row in triangle:
        print(" ".join(map(str, row)))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pascals_triangle',
    default_args=default_args,
    description='DAG для генерации треугольника Паскаля',
    start_date=datetime(2023, 10, 1),
    schedule_interval='@daily',
    catchup=False,  # Добавлено для предотвращения выполнения всех прошедших интервалов
)

task = PythonOperator(
    task_id='generate_and_print_pascals_triangle',
    python_callable=print_pascals_triangle,
    dag=dag,
)
