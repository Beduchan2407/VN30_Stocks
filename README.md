## Descroption
* In stock trading, there are some aspert that we need consider before making stock exchanges. It's stock' price, volume, RSI index,... But there is one aspert that we can not ignore, it's market direction. In Uptrend direction, 75% of stocks will increase in price, so this is the appropriate time to make stock exchange. But in DownTrend direction, 75% of stocks will decrease in price and in this time we should step out of market, waiting for the next Uptrend direction. As you can see, identify the market direction is very important if we want to make money from stock market.
* This repo provides the ETL, to ingest the price of top30 of stocks in VietNam's market into a database. The purpose of this database is to indentify the market's direction based on MA10, MA20, MA50 lines. 
## Data source
* There are two data sources, the new_data and the full_data. 
* The new_data is the close price of stock today. It will be extracted from API of VPS's priceboard, and will be done by using the Scrapy tool.
* The API contain a lot of data, but we just need some of it, such as "Symbol", "Close price", "Volume", "Date'.
* The full_data is the historical close price from 2019 to yesterday. 
## Database

## ETL Pipline

* Check_the_API task: check the API is exists or not
* Extract_data task: Extract data from API and save it as csv file
* Branch task: Identify the next task based on day of week. If the day is weekdays then move to 'spark_process_weekdays' task, otherwise move to 'spark_process_weekends' task
* spark_process_weekdays task: Combine the new_data we that just extracted in previous step with the full_data to create the new full_data, then processing it and save the result as csv file. The new full_data is going to be saved too.
* spark_process_weekends task: We just combine the new_data with the full_data to create the new full_data and save it.
* Update_time task: Insert new date into date_dim table
* Update_data task: Insert new prosessed data into to data_fact table

 

