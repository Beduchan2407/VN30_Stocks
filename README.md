## Descroption
* In stock trading, there are some aspert that we need consider before making stock exchanges. It's stock' price, volume, RSI index,... But there is one aspert that we can not ignore, it's market direction. In Uptrend direction, 75% of stocks will increase in price, so this is the appropriate time to make stock exchange. But in DownTrend direction, 75% of stocks will decrease in price and in this time we should step out of market, waiting for the next Uptrend direction. As you can see, identify the market direction is very important if we want to make money from stock market.
* This repo provides the ETL, to ingest the price of top30 of stocks in VietNam's market into a data warehouse. The purpose of this data warehouse is to indentify the market's direction based on MA10, MA20, MA50 lines. 
## Data source
* There are two data sources, the new daily data and the old data. 
* The new daily data is the close price of stock everyday. It will be extracted from API of VPS's priceboard, and will be done by using the Scrapy tool.
* The API contain a lot of data, but we just need some of it, such as "Symbol", "Close price", "Volume", "Date'.
* The old data is the historical close price from 2019 to yesterday. 

