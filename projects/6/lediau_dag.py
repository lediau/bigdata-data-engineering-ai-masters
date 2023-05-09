import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
import os



default_args = {
    'start_date': days_ago(0),
    'depends_on_past': False,
}

base_dir = '{{ dag_run.conf["base_dir"] if dag_run else "" }}'
#path = os.path.join(args.path_out, output_path)
with DAG(
    'lediau_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
    ) as dag:
    
    feature_eng_task = SparkSubmitOperator(
        application=os.path.join(base_dir, 'preprocess.py')\
        #application="~/ozon-masters-bigdata/projects/6/preprocess.py"\
        , task_id="feature_eng_train_task"\
        , application_args = ['--path-in', '/datasets/amazon/all_reviews_5_core_train_extra_small_sentiment.json', '--path-out', 'lediau_train_out']\
        ,spark_binary="/usr/bin/spark-submit"\
        ,env_vars={"PYSPARK_PYTHON": '/opt/conda/envs/dsenv/bin/python'}
    )
    
    train_download_task = BashOperator(
        task_id='download_train_task',
        bash_command=f"hdfs dfs -getmerge lediau_train_out {os.path.join(base_dir, 'lediau_train_out_local')}"#{base_dir}lediau_train_out_local",
    )
    
    
    train_task = BashOperator(
        task_id='train_task',
        bash_command=f'{"/opt/conda/envs/dsenv/bin/python"} {os.path.join(base_dir, "train.py")} --train-in {os.path.join(base_dir, "lediau_train_out_local")} --sklearn-model-out {os.path.join(base_dir, "6.joblib")}',
        #bash_command=f'python {base_dir}train.py --train-in {base_dir}lediau_train_out_local --sklearn-model-out {base_dir}6.joblib',
    )
    
    model_sensor = FileSensor( task_id= "model_sensor", filepath= os.path.join(base_dir, "6.joblib") )
    
    feature_eng_task_test = SparkSubmitOperator(
        application=os.path.join(base_dir, "preprocess.py")\
        , task_id="feature_eng_test_task"\
        , application_args = ['--path-in', '/datasets/amazon/all_reviews_5_core_test_extra_small_features.json', '--path-out', 'lediau_test_out']\
        ,spark_binary="/usr/bin/spark-submit"\
        ,env_vars={"PYSPARK_PYTHON": '/opt/conda/envs/dsenv/bin/python'}
    )

    predict_task = SparkSubmitOperator(
        application=os.path.join(base_dir, "predict.py")\
        , task_id="predict_task"\
        , application_args = ['--train-in', 'lediau_test_out', '--pred-out', 'lediau_hw6_prediction', '--sklearn-model-in', f'{os.path.join(base_dir, "6.joblib")}']\
        ,spark_binary="/usr/bin/spark-submit"\
        ,env_vars={"PYSPARK_PYTHON": '/opt/conda/envs/dsenv/bin/python'}
    ) 
    

feature_eng_task >>  train_download_task >> train_task >> model_sensor >> feature_eng_task_test >> predict_task
