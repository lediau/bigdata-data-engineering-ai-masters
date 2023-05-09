from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

base_dir = '/opt/airflow/airflow_home/dags/example'
pyspark_python = "/opt/conda/envs/dsenv/bin/python"

with DAG(
        dag_id="lediau_dag",
        start_date=datetime(2023, 5, 9),
        schedule=None,
        catchup=False,
        description="Say no to description",
        doc_md = "Say no to documentation",
        tags=["example"],
) as dag:
    feature_eng_train_task = BashOperator(
        task_id='feature_eng_train_task',
        bash_command=f'conda activate dsenv; python {dag.conf["base_dir"]}/transformer.py --path-in /datasets/amazon/all_reviews_5_core_train_extra_small_sentiment.json --path-out lediau_train_out'
    )

    download_train_task = BashOperator(
        task_id='download_train_task',
        bash_command=f'hdfs dfs -get lediau_train_out {dag.conf["base_dir"]}/lediau_train_out_local'
    )

    train_task = BashOperator(
        task_id='train_task',
        bash_command=f'conda activate dsenv; python {dag.conf["base_dir"]}/train.py --path-in {dag.conf["base_dir"]}/lediau_train_out_local --sklean-model-out {dag.conf["base_dir"]}/6.joblib'
    )

    model_sensor = FileSensor(
        task_id='model_sensor',
        filepath=f'{base_dir}/6.joblib',
        poke_interval=30,
        timeout=60 * 5,
    )

    feature_eng_test_task = BashOperator(
        task_id='feature_eng_test_task',
        bash_command=f'conda activate dsenv; python {dag.conf["base_dir"]}/transformer.py --path-in /datasets/amazon/all_reviews_5_core_test_extra_small_features.json --path-out lediau_train_out'
    )

    predict_task = BashOperator(
        task_id='predict_task',
        bash_command='echo predict_task'
    )

    download_train_task >> feature_eng_train_task >> train_task
    feature_eng_test_task >> model_sensor_task >> predict_task

    # sensor_task = FileSensor(
    #     task_id=f'sensor_task',
    #     filepath=f"{base_dir}/some_file",
    #     poke_interval=30,
    #     timeout=60 * 5,
    # )

    # spark_task = SparkSubmitOperator(
    #     task_id="spark_task",
    #     application=f"{base_dir}/spark_example.py",
    #     spark_binary="/usr/bin/spark3-submit",
    #     num_executors=10,
    #     executor_cores=1,
    #     executor_memory="2G",
    #     env_vars={"PYSPARK_PYTHON": pyspark_python},
    # )

    # sensor_task >> bash_task >> spark_task
