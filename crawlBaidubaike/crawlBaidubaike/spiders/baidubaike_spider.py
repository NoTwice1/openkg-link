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
#url
        url = response.url
        if url == BAIDUBAIKE_ERROR_URL or url == BAIDUBAIKE_ROOT_URL:
            print 'r.url = ' + url + 'invalid'
            return
        baidubaikeItem['url'] = url

        print time.ctime()+'-'*5+'crawl %d : %s' % (self.crawlingCount, url)
#title
        titleNode = response.xpath('.//dd[@class="lemmaWgt-lemmaTitle-title"]')
        h1 = titleNode.xpath('.//h1/text()').extract()[0]
        title = h1
        h2 = titleNode.xpath('.//h2/text()').extract()
        if len(h2) > 0:
            h2 = h2[0]
            title += h2
        # print title
        # print '-'*20 + 'title' + '-'*20
        baidubaikeItem['title'] = title
#content
        paras = response.xpath('.//div[@class="para"]')
        content = ''
        if len(paras)!= 0:
            for para in paras:
                texts = para.xpath('.//text()').extract()
                for text in texts:
                    content += text
        # print content
        # print '-'*20 + 'content' + '-'*20
            baidubaikeItem['content'] = content
        else:
            # print 'no content'
            baidubaikeItem['content'] = None            
#infobox
        infobox = {}
        infoboxNode = response.xpath('.//div[@class="basic-info cmn-clearfix"]')
        if len(infoboxNode) != 0:
            infobox_names = infoboxNode.xpath('.//dt//text()').extract()
            infobox_valuesNode = infoboxNode.xpath('.//dd')
            infobox_values = []
            for i in infobox_valuesNode:
                l = i.xpath('.//text()').extract()
                value = ''
                for j in l:
                    value = value + j.strip()
                infobox_values.append(value.encode('utf-8'))
            for i in range(len(infobox_names)):
                # print infobox_names[i] + ':' + infobox_values[i] 
                # print unicode(infobox_names[i],'gbk').encode('utf-8')  + ':'
                # print infobox_values[i].strip()
                infobox[infobox_names[i].strip()] = infobox_values[i].strip()
            baidubaikeItem['infobox'] = infobox
        else:
            # print 'no infobox'
            baidubaikeItem['infobox'] = None
#category
        categoryNode  = response.xpath('//div[@class="lemmaWgt-lemmaCatalog"]')
        if len(categoryNode) != 0:
            category = categoryNode.xpath('.//a//text()').extract()
            # print type(category)
            baidubaikeItem['category'] = category
            # print category
            # print '-'*20 + 'category' + '-'*20
        else :
            # print 'no category'
            baidubaikeItem['category'] = None
#reference
        reference = {}
        referenceNode = response.xpath('//li[@class="reference-item "]//a[@rel="nofollow"]')
        if len(referenceNode)!=0:
            for i in referenceNode:
                name = i.xpath('.//text()')[0].extract()
                link = i.xpath('.//@href')[0].extract()
                reference[name] = link
                baidubaikeItem['reference'] = reference
            # for k,v in reference.iteritems():
            #     print k + ':' +v
            # print '-'*20 + 'reference' +'-'*20

        else:
            # print 'no reference'
            baidubaikeItem['reference'] = None

#tag
        tag = []
        tagNode = response.xpath('//span[@class="taglist"]//text()').extract()
        if len(tagNode) != 0:
            for i in tagNode:
                tag.append(i.strip())
            baidubaikeItem['tag'] = tag
            # print tag
            # print '-'*20 + 'tag' +'-'*20
        else:
            # print 'no tag'
            baidubaikeItem['tag'] = None


#image_url
        image_urlNode = response.xpath('//div[@class="summary-pic"]//img//@src')
        if len(image_urlNode) != 0:
            image_url = image_urlNode[0].extract()
            baidubaikeItem['image_url'] = image_url
        else:
            baidubaikeItem['image_url'] = None
        # print image_url
        # print '-'*20 + 'image_url' +'-'*20

#inner_related_urls
        urls = response.xpath("//a[contains(@href,'/item/')]/@href").extract()
        baidubaikeItem['inner_related_urls'] = urls
        # print 'end collect'

        yield baidubaikeItem

        # print '-----------------------------------------------------------------'

        for url in urls:
            # 将信息组合成下一页的url
            # if url in self.new_url_list or url in self.new_url_list:
            #     continue
            page = response.urljoin(url)
            # print urllib.unquote(page)
            # 返回url
            yield scrapy.Request(page, callback=self.parse)