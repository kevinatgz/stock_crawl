#encoding:utf-8
'''
Created on Mar 24, 2019

@author: kevin
'''
import csv
import pymongo
import time

try:
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client.stock_news
    collection = db.stocks
    with open('../usstock/companylist2.csv','rb') as myFile:
        lines=csv.reader(myFile)
        count=0
        for line in lines:
            if count==0 :
                count+=1
                continue
#             print line    
            stock = {
                'Symbol': line[0].strip(),
                'Name': line[1],
                'Sector':line[5],
                'Industry': line[6],
                'SummaryUrl':line[7],
                'MarketCap':line[3],
                'IPOYear':line[4],
                'Type':'US',
                'LastSale':line[2],
                'Time': time.time()                
            }
            print (stock)
            count+=1
            result = collection.insert(stock)
            print(result)

except :
     
    print " Error " 


