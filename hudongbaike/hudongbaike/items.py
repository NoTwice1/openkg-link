# -*- coding: utf-8 -*-
import scrapy


class HudongbaikeItem(scrapy.Item):
    title = scrapy.Field()
    abstract = scrapy.Field()
    content = scrapy.Field()
    url = scrapy.Field()
    item_id = scrapy.Field()

