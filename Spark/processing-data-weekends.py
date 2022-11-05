import findspark
findspark.init()
import pandas as pd
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.types import *
from datetime import date, timedelta

spark = SparkSession\
    .builder\
    .master("local")\
    .appName("processing-data")\
    .getOrCreate()

today = date.today()
yesterday = today - timedelta(1)

full_data = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("dateFormat", "yyyy-MM-dd")\
    .option("path", f"Spark_project/Stock/Daily_data/Full_data/{yesterday.year}/{yesterday.month}/{yesterday.day}/*.csv")\
    .load()

new_data = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("dateFormat", "yyyy-MM-dd")\
    .option("inferSchema", "true")\
    .option("path", f"Spark_project/Stock/Daily_data/New_data/{today.year}/{today.month}/{today.day}/*.csv")\
    .load()


full_data = full_data\
    .withColumn("Date", full_data.Date.cast("date"))\
    .select("Symbol", "Date", "Close", "Volume", "Dayoff")

new_data = new_data\
    .withColumn("Date", new_data.Date.cast("date"))\
    .withColumn("Dayoff", f.lit("Yes"))\
    .select("Symbol", "Date", "Close", "Volume", "Dayoff")

new_full_data = full_data.union(new_data)

# Save new full_data
new_full_data = new_full_data.repartition(1)
full_data_path_day = f"Spark_project/Stock/Daily_data/Full_data/{date.today().year}/{date.today().month}/{date.today().day}"
full_data_path_month = f"Spark_project/Stock/Daily_data/Full_data/{date.today().year}/{date.today().month}"
full_data_path_year = f"Spark_project/Stock/Daily_data/Full_data/{date.today().year}"

if not os.path.exists(full_data_path_day):
    if today.day == 1: # new month
        if today.month == 1: # new year
            os.mkdir(full_data_path_year)
            os.mkdir(full_data_path_month)
            os.mkdir(full_data_path_day)
        else:
            os.mkdir(full_data_path_month)
            os.mkdir(full_data_path_day)
    else:
        os.mkdir(full_data_path_day)
new_full_data.toPandas().to_csv(f"{full_data_path_day}/Full_data.csv", index=False)
