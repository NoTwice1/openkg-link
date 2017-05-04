# -*- coding:utf-8 -*-
import os
import urllib2
from bs4 import BeautifulSoup
import re
import requests


def fenlei():
    fenleiList = []
    url = 'https://baike.baidu.com/'
    result = urllib2.urlopen(url).read()
    # windows
    # html_doc = unicode(result).encode('utf-8')
    # linux
    html_doc = result
    soup = BeautifulSoup(html_doc, 'html.parser')
    columns = soup.find_all('div', class_='column')
    for column in columns:
        hrefs = column.find_all('a')
        for href in hrefs:
            url = href['href']
            full_url = "http://baike.baidu.com" + url
            # print full_url
            fenleiList.append(full_url)
    return fenleiList


def start_urls():
    startUrls = []
    fenleiList = fenlei()
    for full_url in fenleiList:
        print full_url
        result = urllib2.urlopen(full_url.encode('utf-8')).read()
        html_doc = result
        soup = BeautifulSoup(html_doc, 'html.parser')
        urls = soup.find_all('a', href=re.compile(r'/view/\d+\.htm'))
        for url in urls:
            startUrls.append('http://baike.baidu.com' + url['href'])
    print 'collect start_urls end total number:%d'%len(startUrls)
    return startUrls

def save_urls():
    startUrls = start_urls()
    with open('start_urls.txt', 'w') as f:
        for i in startUrls:
            f.write(i.encode('utf-8') + '\n')
    print 'save ok'

def read_urls():
    startUrls = []
    with open('start_urls.txt') as f:
        for line in f:
            startUrls.append(line.strip().decode('utf-8'))
    print 'read startUrls ok'
    return startUrls

if __name__ == '__main__':
    read_urls()
