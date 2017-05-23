# -*- coding: utf-8 -*-
import scrapy
from hudongbaike.items import HudongbaikeItem
import os

class MainSpider(scrapy.Spider):
    name = "hudong"
    allowed_domains = ["www.baike.com"]
    start_urls = ["http://www.baike.com/wiki/%E6%AF%9B%E6%B3%BD%E4%B8%9C",
                  "http://www.baike.com/wiki/%E4%BA%BA%E5%B7%A5%E6%99%BA%E8%83%BD",
                  "http://www.baike.com/wiki/%E9%9F%B3%E4%B9%90",
                  "http://www.baike.com/wiki/%E8%96%9B%E5%AE%9A%E8%B0%94%E7%9A%84%E7%8C%AB",
                  "http://www.baike.com/wiki/%E4%B8%AD%E5%9B%BDC919%E5%A4%A7%E5%9E%8B%E5%AE%A2%E6%9C%BA",
                  "http://www.baike.com/wiki/%E6%BB%B4%E6%BB%B4%E5%87%BA%E8%A1%8C",
                    ]

    def __init__(self):
        super(scrapy.Spider, self).__init__()
        self.item_id = self._get_item_id()

    def _get_item_id(self):
        id_file = "./last_id.txt"
        if os.path.exists(id_file):
            item_id = int(open(id_file).readline())
        else:
            item_id = 0
        return item_id

    def _clean_text(self, x):
        x = x.replace("\t","")
        x = x.replace("\n","")
        if x == "":
            x = " "
        return x

    def get_external_url(self, x): #like: u"goUrl('http://www.manshijian.com/articles/article_detail/8575.html')"
        return x[7:-2]

    def parse(self, response):
        name = response.selector.xpath("//div[@class='content-h1']/h1/text()").extract()[0]  #页面标题|实体名称
        abstract = "".join(response.selector.xpath("//div[@class='summary']/p//text()").extract()) #摘要

        content = {}  #主题内容 key是小标题，value是对应的一段文本
        subtitles = response.selector.xpath("//div[@class='content_h2']/h2/text()[2]").extract()
        lens = len(subtitles)
        for i in range(lens):
            text = response.selector.xpath(
                "//div[@id='content']//text()[count(preceding::div[@class='content_h2'])={}]".format(i+1)
            ).extract()
            text = map(lambda x:self._clean_text(x), text)

            if i != lens-1:
                text = "".join(text[:-3])
            else:
                text = "".join(text)

            content[subtitles[i]] = text

        infobox = {}
        attributes = response.selector.xpath("//div[@class='module zoom']//strong/text()").extract()
        values = response.selector.xpath("//div[@class='module zoom']//span/text()").extract()
        for a,v in zip(attributes,values):
            infobox[a] = v

        new_urls = response.selector.xpath("//div[@class='summary']/p/a/@href").extract()
        new_urls.extend(response.selector.xpath("//div[@id='content']//a/@href").extract())

        external_urls = response.selector.xpath(
            "//dl[@class='reference bor-no']//a[@onclick]/@onclick").extract()  #参考文献
        external_urls = map(lambda x:self.get_external_url(x), external_urls)

        tags = response.selector.xpath("//dl[@id='show_tag']/dd/a/text()").extract()  #分类标签

        zishi = response.selector.xpath("//div[@class='dnancon']//p/text()").extract() #“百科帮你涨姿势”

        wenxian = {}  #相关文献
        wenxian_title = response.selector.xpath("//div[@class='wenxian_app']//a/@title").extract()
        wenxian_url = response.selector.xpath("//div[@class='wenxian_app']//a/@href").extract()
        for t,u in zip(wenxian_title,wenxian_url):
            wenxian[t] = u


        item = HudongbaikeItem()
        item['name'] = name
        item['abstract'] = abstract
        item['content'] = content
        item['url'] = response.url
        item['infobox'] = infobox
        item['external_urls'] = external_urls
        item['internal_urls'] = filter(lambda x:"www.baike.com" in x, new_urls)
        item['image_urls'] = filter(lambda x:"tupian.baike.com" in x, new_urls)
        item['tags'] = tags
        item['zishi'] = zishi
        item['wenxian'] = wenxian

        item['item_id'] = self.item_id
        self.item_id += 1
        if not(self.item_id % 10):
            print self.item_id
        yield item

        for url in item['internal_urls']:
            yield scrapy.Request(url, callback=self.parse)

    def close(self, reason):
        f = open("./last_id.txt","w")
        f.write(str(self.item_id))