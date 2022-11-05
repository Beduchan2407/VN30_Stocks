# -*- coding: utf-8 -*-
import scrapy
import json
import datetime

class StocksSpider(scrapy.Spider):
    name = 'stocks'
    allowed_domains = ['https://banggia.vps.com.vn/']
    start_urls = ['https://bgapidatafeed.vps.com.vn/getliststockdata/ACB,BID,BVH,CTG,FPT,GAS,GVR,HDB,HPG,KDH,MBB,MSN,MWG,NVL,PDR,PLX,POW,SAB,SSI,STB,TCB,TPB,VCB,VHM,VIB,VIC,VJC,VNM,VPB,VRE']

    def parse(self, response):
        #convert string to array of dict
        raw_data = list(eval(response.body))
        for i in raw_data:
            yield{"Symbol": i["sym"], "Close": int(float(i["lastPrice"])*1000), "Volume": int(i["lot"]), "Date": datetime.date.today().strftime("%Y-%m-%d")}

        


            
