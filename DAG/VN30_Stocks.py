import imp
import os
from datetime import datetime, date
from airflow.models import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import date, datetime

tabDays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

def branch():
    if tabDays[datetime.now().weekday()] in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
        return "spark_process_weekdays"
    else:
        return "spark_process_weekends"


with DAG(
    dag_id="VN30_Stocks",
    start_date=datetime(year=2020, month=11, day=4),
    schedule_interval=None,
    catchup=False
) as dag:

    # Start task
    task_start = DummyOperator(
        task_id = "Start",
    )

    # End task
    task_end = DummyOperator(
        task_id = "End",
        trigger_rule = "all_done"
    )

    # Check the API
    task_check_api = HttpSensor(
        task_id = "Check_the_API",
        http_conn_id="VN30_API",
        endpoint="/getliststockdata/ACB,BID,BVH,CTG,FPT,GAS,GVR,HDB,HPG,KDH,MBB,MSN,MWG,NVL,PDR,PLX,POW,SAB,SSI,STB,TCB,TPB,VCB,VHM,VIB,VIC,VJC,VNM,VPB,VRE"
    )

    # Get the new daily data 
    task_extract_data = BashOperator(
        task_id = "Extract_data",
        bash_command=f"cd /home/minhhieu/Spark_project/Stock/Scrapy/vn_stock && scrapy crawl stocks -o /home/minhhieu/Spark_project/Stock/Daily_data/New_data/{date.today().year}/{date.today().month}/{date.today().day}/Newdata.csv"
    )

    # Processing weekdays data
    task_processing_data_weekdays = SparkSubmitOperator(
        task_id = "spark_process_weekdays",
        conn_id="spark_local",
        application="/home/minhhieu/Spark_project/Stock/Spark/processing-data-weekdays.py",
    )
    
    # Processing weekends data
    task_processing_data_weekends = SparkSubmitOperator(
        task_id = "spark_process_weekends",
        conn_id="spark_local",
        application="/home/minhhieu/Spark_project/Stock/Spark/processing-data-weekends.py",
    )

    # Update Date_Dim table
    task_update_time = PostgresOperator(
        task_id = "Update_time",
        postgres_conn_id="VN30_DW",
        sql=f"""
            insert into date_dim
            values ('{date.today().year}-{date.today().month}-{date.today().day}', '{tabDays[datetime.now().weekday()]}', {date.today().day}, {date.today().month}, {date.today().year}) 
        """
    )

    # Update data_fact table
    task_update_data = PostgresOperator(
        task_id = "Update_data",
        postgres_conn_id="VN30_DW",
        sql=f"""
            copy data_fact(symbol, date, close, volume, ma10, ma20, ma50, price_change_in_day, price_change_in_week, price_change_in_month, price_change_in_6month, price_change_in_year)
            from '/home/minhhieu/Spark_project/Stock/Processed_data/{date.today().year}/{date.today().month}/{date.today().day}/Processed_data.csv'
            delimiter ','
            csv HEADER
        """
    )

    # Branch
    task_branch = BranchPythonOperator(
        task_id = "Branch",
        python_callable=branch
    )

    # Pipline
    task_start >> task_check_api >> task_extract_data >> task_branch >> [task_processing_data_weekdays, task_processing_data_weekends]
    task_processing_data_weekends >> task_end
    task_processing_data_weekdays >> task_update_time >> task_update_data >> task_end

    
