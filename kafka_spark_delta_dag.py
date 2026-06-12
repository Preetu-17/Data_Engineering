from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator	

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='kafka_to_delta_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    
    Bronze = SparkSubmitOperator(
        task_id='Ingest_to_Bronze',
        conn_id='spark',
        application='/home/preetham/Data_Lake/Scripts/bronze_job.py',
	packages='org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0',
        verbose=True
    )

    Silver = SparkSubmitOperator(
        task_id='Transform_Bronze_to_Silver',
        conn_id='spark',
        application='/home/preetham/Data_Lake/Scripts/silver_job.py',
	packages='org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0',
        verbose=True
    )

    Gold = SparkSubmitOperator(
        task_id='Aggregate_Silver_to_Gold',
        conn_id='spark',
        application='/home/preetham/Data_Lake/Scripts/gold_job.py',
	packages='org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0',
        verbose=True
    )

    Audit = BashOperator(
        task_id="Audit_Monitoring",
        bash_command="""
        spark-submit /home/preetham/Data_Lake/Operations/audit_job.py
        """
    )

    Data_Quality = BashOperator(
        task_id="Data_Quality",
        bash_command="""
        spark-submit /home/preetham/Data_Lake/Operations/data_quality_job.py
        """
    )

    Lineage = BashOperator(
        task_id="Lineage",
        bash_command="""
        spark-submit /home/preetham/Data_Lake/Operations/lineage_job.py
        """
    )

    Bronze >> Silver >> Gold >> Audit >> Data_Quality >> Lineage

