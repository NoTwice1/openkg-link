# -*- coding: utf-8 -*-
import scrapy


class HudongbaikeItem(scrapy.Item):
    name = scrapy.Field()
    abstract = scrapy.Field()
    content = scrapy.Field()
    url = scrapy.Field()
    item_id = scrapy.Field()
    infobox = scrapy.Field()
    internal_urls = scrapy.Field()
    external_urls = scrapy.Field()
    image_urls = scrapy.Field()
    tags = scrapy.Field()
    zishi = scrapy.Field()
    wenxian = scrapy.Field()

