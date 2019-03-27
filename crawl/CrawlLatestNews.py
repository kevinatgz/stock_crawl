#encoding:utf-8
'''
Created on Mar 24, 2019

@author: kevin
'''
from kafka import KafkaProducer
import json
import pymongo
import random 
import time
import ssl
import MySQLdb
import re
import hashlib
import urllib2
import cookielib

producer1 = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: json.dumps(m).encode('ascii'))

def crawlLatestNews(symbol,client):  
#     client = pymongo.MongoClient(host='localhost', port=27017)
    randomNum = ''.join(str(random.choice(range(10))) for _ in range(12)) 
    rand = str(random.random())
    m = hashlib.md5() #创建Md5对象
    m.update(rand.encode('utf-8')) 
    rand2 = m.hexdigest() #经过md5加密的字符串赋值
    

    db = client.stock_news
    collection = db.stock_news_crawl
    ssl._create_default_https_context = ssl._create_unverified_context
    urllib_opener = urllib2.build_opener()

    #拿到一个cookie实例，用来保留cookie，具体怎么保留这个不用操心，一切给http handler(这里就是HTTPCookieProcessor)处理
#     cookie = cookielib.CookieJar()
    #获取一个保存cookie的对象
    cj = cookielib.LWPCookieJar()
    #将一个保存cookie对象，和一个HTTP的cookie的处理器绑定
    cookie_support = urllib2.HTTPCookieProcessor(cj)
    #创建一个opener，将保存了cookie的http处理器，还有设置一个handler用于处理http的URL的打开
    opener = urllib2.build_opener(cookie_support, urllib2.HTTPHandler)
    opener.addheaders.append( ( 'Cookie', 'machine_cookie=6'+randomNum ) )
    #将包含了cookie、http处理器、http的handler的资源和urllib2对象板顶在一起
    urllib2.install_opener(opener)        
    
    headers = {'Accept-Language': 'en-US,en;q=0.8','Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3','Accept-Encoding': 'none','Connection': 'keep-alive','Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3','User-Agent':'Mozilla/5.0 (Windows;U;Windows NT 5.1;zh-CN;rv:1.9.2.9)Gecko/20100824 Firefox/3.6.9','Referer':''}    
    # req = urllib2.Request(url = 'https://seekingalpha.com/symbol/GOOG?s=goog',headers = headers)
    url = 'https://seekingalpha.com/symbol/'+symbol+'?s='+symbol
    print url
    request = urllib2.Request(url ,headers = headers)
    content = opener.open(request).read()    
    opener.close()
    #"非贪婪匹配,re.S('.'匹配字符,包括换行符)"
    links1=re.findall(r'<ul id="symbol-page-latest">(.*?)</ul>',content,re.S)
    # print(links1)
    links=re.findall(r'<a href="(.*?)" sasource="qp_latest">(.*?)</a>',links1[0],re.S)
    for url in links:
        link='https://seekingalpha.com'+url[0]
           
#         print url
        news = {
            'stock_code': str(symbol),
            'title': url[1],
#             'content': '',
            'url':link,
            'time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }
        resultI = collection.insert(news)
        news.pop('_id',None)
        print(resultI)   
        print(news)
        future1 = producer1.send('test' ,key= str(symbol),  value= news, partition= 0)
        future1.get(timeout= 10)
#                  
#         print('')
    
    
# try:
client = pymongo.MongoClient(host='localhost', port=27017)
db = client.stock_news
collection = db.stocks

results = collection.find({'Type': 'US','Sector': 'Technology'})
#     print(results)
for result in results:
    print(result)
    crawlLatestNews(result['Symbol'].strip(),client)
    time.sleep(random.randint(7, 10) )
        
# except Exception,err:     
#     print 1,err
