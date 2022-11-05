import findspark
findspark.init()
import pandas as pd
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.types import *
from datetime import date, timedelta

@f.pandas_udf(FloatType())
def percent_change(v: pd.Series) -> float:
    first_row = v.iloc[[0][0]]
    if len(v) == 1:
        last_row = v.iloc[[0][0]]
    else:
        last_row = v.iloc[[len(v)-1][0]]

    return (last_row - first_row) / first_row * 100


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
    .withColumn("Dayoff", f.lit("No"))\
    .select("Symbol", "Date", "Close", "Volume", "Dayoff")

new_full_data = full_data.union(new_data)

# create window for MA caculations
running_avg_window_10 = Window.partitionBy("Symbol").orderBy(f.col("Date").asc()).rowsBetween(Window.currentRow - 10, Window.currentRow)
running_avg_window_20 = Window.partitionBy("Symbol").orderBy(f.col("Date").asc()).rowsBetween(Window.currentRow - 20, Window.currentRow)
running_avg_window_50 = Window.partitionBy("Symbol").orderBy(f.col("Date").asc()).rowsBetween(Window.currentRow - 50, Window.currentRow)

# create window for price_change caculations in one day, one week, one month, 6 month, one year
running_avg_window_1 = Window.partitionBy("Symbol").orderBy(f.col("Date").asc()).rowsBetween(Window.currentRow - 1, Window.currentRow)
running_avg_window_7 = Window.partitionBy("Symbol").orderBy(f.col("Date").asc()).rowsBetween(Window.currentRow - 7, Window.currentRow)
running_avg_window_31 = Window.partitionBy("Symbol").orderBy(f.col("Date").asc()).rowsBetween(Window.currentRow - 31, Window.currentRow)
running_avg_window_183 = Window.partitionBy("Symbol").orderBy(f.col("Date").asc()).rowsBetween(Window.currentRow - 183, Window.currentRow)
running_avg_window_365 = Window.partitionBy("Symbol").orderBy(f.col("Date").asc()).rowsBetween(Window.currentRow - 365, Window.currentRow)


agg_MA_data = new_full_data\
    .where(f.col("Dayoff") == "No")\
    .withColumn("MA10", f.round(f.avg("Close").over(running_avg_window_10), 2))\
    .withColumn("MA20", f.round(f.avg("Close").over(running_avg_window_20), 2))\
    .withColumn("MA50", f.round(f.avg("Close").over(running_avg_window_50), 2))\
    .where(f.col("Date").cast("string") == f"{today.strftime('%Y-%m-%d')}")

agg_Price_change_data = new_full_data\
    .withColumn("Price_change_in_day", f.round(percent_change("Close").over(running_avg_window_1), 2))\
    .withColumn("Price_change_in_week", f.round(percent_change("Close").over(running_avg_window_7), 2))\
    .withColumn("Price_change_in_month", f.round(percent_change("Close").over(running_avg_window_31), 2))\
    .withColumn("Price_change_in_6mongth", f.round(percent_change("Close").over(running_avg_window_183), 2))\
    .withColumn("Price_change_in_year", f.round(percent_change("Close").over(running_avg_window_365), 2))\
    .select("Symbol", "Date", "Price_change_in_day", "Price_change_in_week", "Price_change_in_month", "Price_change_in_6mongth", "Price_change_in_year")\
    .where(f.col("Date").cast("string") == f"{today.strftime('%Y-%m-%d')}")


new_processed_data = agg_MA_data.join(agg_Price_change_data, (agg_MA_data.Symbol == agg_Price_change_data.Symbol) & (agg_MA_data.Date == agg_Price_change_data.Date), "inner")\
    .select(agg_MA_data.Symbol, agg_MA_data.Date, "Close", "Volume", "MA10","MA20", "MA50", "Price_change_in_day", "Price_change_in_week", "Price_change_in_month", "Price_change_in_6mongth", "Price_change_in_year")

# Save new processed data
new_processed_data = new_processed_data.repartition(1)
processed_data_path_day = f"Spark_project/Stock/Processed_data/{date.today().year}/{date.today().month}/{date.today().day}"
processed_data_path_month = f"Spark_project/Stock/Processed_data/{date.today().year}/{date.today().month}"
processed_data_path_year = f"Spark_project/Stock/Processed_data/{date.today().year}"

if not os.path.exists(processed_data_path_day):
    if today.day == 1: # new month
        if today.month == 1: # new year
            os.mkdir(processed_data_path_year)
            os.mkdir(processed_data_path_month)
            os.mkdir(processed_data_path_day)
        else:
            os.mkdir(processed_data_path_month)
            os.mkdir(processed_data_path_day)
    else:
        os.mkdir(processed_data_path_day)
new_processed_data.toPandas().to_csv(f"{processed_data_path_day}/Processed_data.csv", index=False)

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
