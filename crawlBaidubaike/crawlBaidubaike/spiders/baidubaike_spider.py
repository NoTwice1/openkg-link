# -*- coding:utf-8 -*-
import random

import scrapy
import urllib
import thread
import urllib2
import time
from collectStartUrls import start_urls
from crawlBaidubaike.baidubaikeItem import BaidubaikeItem

BAIDUBAIKE_ERROR_URL = 'http://baike.baidu.com/error.html'
BAIDUBAIKE_ROOT_URL = 'https://baike.baidu.com/'
STRAT_UIL = "http://baike.baidu.com/item/python"


class BaidubaikeSpider(scrapy.Spider):
    # 设置name
    name = "baidubaikeSpider"
    # 设定域名
    allowed_domains = ["baike.baidu.com"]
    # 填写爬取地址
    start_urls = start_urls()

    def __init__(self):
        # self.proxies = self.load_proxies()
        # self.currentProxy = None
        # self.change_proxy()
        # self.count = 0
        self.crawlingCount = 0

    #     self.new_url_list = set()
    #     self.old_url_list = set()
    #     self.new_url_list.add(STRAT_UIL)

    # 编写爬取方法
    def parse(self, response):
        self.crawlingCount += 1
        # self.count += 1
        # if self.count == 20:
        #     self.change_proxy()
        #     self.count = 0
        # 实例一个容器保存爬取的信息
        baidubaikeItem = BaidubaikeItem()
        # 这部分是爬取部分，使用xpath的方式选择信息，具体方法根据网页结构而定
        # 先获取每个课程的div
        # print '----------------------------------------------'
        url = response.url
        if url == BAIDUBAIKE_ERROR_URL or url == BAIDUBAIKE_ROOT_URL:
            print 'r.url = ' + url + 'invalid'
            return
        baidubaikeItem['url'] = url
        print time.ctime()+'--------------crawl %d : %s' % (self.crawlingCount, url)

        # self.new_url_list.remove(url)
        # self.old_url_list.add(url)

        titleNode = response.xpath('.//dd[@class="lemmaWgt-lemmaTitle-title"]')
        h1 = titleNode.xpath('.//h1/text()').extract()[0]
        title = h1
        h2 = titleNode.xpath('.//h2/text()').extract()
        if len(h2) > 0:
            h2 = h2[0]
            title += h2
        baidubaikeItem['title'] = title

        paras = response.xpath('.//div[@class="para"]')
        content = ''
        for para in paras:
            texts = para.xpath('./text()').extract()
            for text in texts:
                content += text
        # print content
        baidubaikeItem['content'] = content
        yield baidubaikeItem

        # print '-----------------------------------------------------------------'

        urls = response.xpath("//a[contains(@href,'/item/')]/@href").extract()
        for url in urls:
            # 将信息组合成下一页的url
            # if url in self.new_url_list or url in self.new_url_list:
            #     continue
            page = response.urljoin(url)
            # print urllib.unquote(page)
            # 返回url
            yield scrapy.Request(page, callback=self.parse)

    # def fenlei(self):
    #     fenleiList = []
    #     url = 'https://baike.baidu.com/'
    #     result = urllib2.urlopen(url).read()
    #     html_doc = unicode(result).encode('UTF-8')
    #     soup = BeautifulSoup(html_doc,'html.parser')
    #     columns = soup.find_all('div',class_ = 'column')
    #     for column in columns:
    #         hrefs = column.find_all('a')
    #         for href in hrefs:
    #             url = href['href']
    #             full_url = "http://baike.baidu.com"+ url
    #             # print full_url
    #         fenleiList.append(full_url)
    #             # yield full_url
    #     return fenleiList

    # def start_urls(self):
    #     start_urls = []
    #     fenleiList = self.fenlei()
    #     # for fenlei in fenlei():
    #     for full_url in fenleiList:
    #         result = urllib2.urlopen(full_url).read()
    #         html_doc = unicode(result).encode('UTF-8')
    #         soup = BeautifulSoup(html_doc,'html.parser')
    #         urls = soup.find_all('a',href=re.compile(r'/view/\d+\.htm'))
    #         for url in urls:
    #             # yield 'http://baike.baidu.com' + url['href']
    #             print 'http://baike.baidu.com' + url['href']
    #             start_urls.append('http://baike.baidu.com' + url['href'])
    #     print 'collect start_urls end'
    #     return start_urls