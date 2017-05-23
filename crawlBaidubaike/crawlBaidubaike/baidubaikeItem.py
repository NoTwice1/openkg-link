# -*- coding:utf-8 -*-
import scrapy


class BaidubaikeItem(scrapy.Item):
    # count = scrapy.Field()
    # url
    url = scrapy.Field()
    # 课程标题
    title = scrapy.Field()
    # 摘要
    summary = scrapy.Field()
    # 文本目录
    category = scrapy.Field()
    # 文本内容
    content = scrapy.Field()
    # infobox
    infobox = scrapy.Field()
    # reference
    reference = scrapy.Field()
    # tag
    tag = scrapy.Field()
    # image
    image_url = scrapy.Field()
    # inner_related
    inner_related_urls = scrapy.Field()

    # main_content = scrapy.Field()

