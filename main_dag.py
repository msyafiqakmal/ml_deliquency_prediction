import airflow
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
import csv_to_pg
import feature_engineering
import dataset_preparation
import ml_in_apache_spark


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 3, 20),
    'retries': 2,

}

dag = DAG('CAPSTONE4', default_args=default_args,schedule_interval=None)

t1 = BashOperator(
    task_id='csv_to_panda_to_postgres',
    bash_command= exec("csv_to_pg"),
    dag=dag)

t2 = BashOperator(
	task_id='feature_engineering',
	bash_command=exec("feature_engineering"),
	dag=dag)

t3 = BashOperator(
    task_id='dataset_preparation_to_csv',
    bash_command=exec("dataset_preparation"),
    dag=dag)
t4 = BashOperator(
    task_id='ml_in_apache_spark',
    bash_command=exec("ml_in_apache_spark"),
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
