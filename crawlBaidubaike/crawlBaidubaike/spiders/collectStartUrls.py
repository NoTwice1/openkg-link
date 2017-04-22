# -*- coding:utf-8 -*-
import urllib2
from bs4 import BeautifulSoup
import re

def fenlei():
    fenleiList = []
    url = 'https://baike.baidu.com/'
    result = urllib2.urlopen(url).read()
    html_doc = unicode(result).encode('UTF-8')
    soup = BeautifulSoup(html_doc,'html.parser')
    columns = soup.find_all('div',class_ = 'column')
    for column in columns:
        hrefs = column.find_all('a')
        for href in hrefs:
            url = href['href']
            full_url = "http://baike.baidu.com"+ url
            # print full_url
        fenleiList.append(full_url)
            # yield full_url
    return fenleiList

def start_urls():
    start_urls = []
    fenleiList = fenlei()
    # for fenlei in fenlei():
    for full_url in fenleiList:
        result = urllib2.urlopen(full_url).read()
        html_doc = unicode(result).encode('UTF-8')
        soup = BeautifulSoup(html_doc,'html.parser')
        urls = soup.find_all('a',href=re.compile(r'/view/\d+\.htm'))
        for url in urls:
            # yield 'http://baike.baidu.com' + url['href']
            # print 'http://baike.baidu.com' + url['href']
            start_urls.append('http://baike.baidu.com' + url['href'])
    print 'collect start_urls end'
    return start_urls