# -*- coding:utf-8 -*-
import scrapy
class BaidubaikeItem(scrapy.Item):
    #   url
    url = scrapy.Field()
    #   课程标题
    title = scrapy.Field()
    #   文本内容
    content = scrapy.Field()