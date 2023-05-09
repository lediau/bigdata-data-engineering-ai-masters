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
    feature_eng_train_task = SparkSubmitOperator(
        task_id="feature_eng_train",
        application=f"{base_dir}/feature_eng.py",
        spark_binary="/usr/bin/spark3-submit",
        num_executors=10,
        executor_cores=1,
        executor_memory="2G",
        application_args=["/datasets/amazon/all_reviews_5_core_train_extra_small_sentiment.json", "lediau_train_out"],
        env_vars={"PYSPARK_PYTHON" : pyspark_python}
    )

    download_train_task = SparkSubmitOperator(
        task_id="download_train",
        application=f"{base_dir}/train_download.py",
        spark_binary="/usr/bin/spark3-submit",
        application_args=["lediau_train_out", f"{base_dir}/lediau_train_out_local"],
        env_vars={"PYSPARK_PYTHON" : pyspark_python}
    )

    train_task = BashOperator(
        task_id="train_model",
        bash_command=f"/opt/conda/envs/dsenv/bin/python {base_dir}/train.py {base_dir}/lediau_train_out_local {base_dir}/6.joblib"
    )

    model_sensor = FileSensor(
        task_id='model_sensor',
        filepath=f'{base_dir}/6.joblib',
        poke_interval=30,
        timeout=60 * 5,
    )

    feature_eng_test_task = SparkSubmitOperator(
        task_id="feature_eng_test",
        application=f"{base_dir}/feature_eng.py",
        spark_binary="/usr/bin/spark3-submit",
        application_args=["/datasets/amazon/all_reviews_5_core_test_extra_small_features.json", "lediau_test_out"],
        env_vars={"PYSPARK_PYTHON" : pyspark_python}
    )

    predict_task = BashOperator(
        task_id='predict_task',
        bash_command='echo predict_task'
    )

    download_train_task >> feature_eng_train_task >> train_task
    feature_eng_test_task >> model_sensor >> predict_task

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
