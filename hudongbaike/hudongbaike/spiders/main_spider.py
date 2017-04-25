import scrapy
from hudongbaike.items import HudongbaikeItem
import os

class MainSpider(scrapy.Spider):
    name = "hudong"
    allowed_domains = ["www.baike.com"]
    start_urls = start_urls = ["http://www.baike.com/wiki/%E6%AF%9B%E6%B3%BD%E4%B8%9C",
                               "http://www.baike.com/wiki/%E4%BA%BA%E5%B7%A5%E6%99%BA%E8%83%BD",
                               "http://www.baike.com/wiki/%E9%9F%B3%E4%B9%90"]

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

    def parse(self, response):
        title = response.selector.xpath("//div[@class='content-h1']/h1/text()").extract()[0]
        abstract = "".join(response.selector.xpath("//div[@class='summary']/p//text()").extract())

        content = a = "".join(response.selector.xpath("//div[@id='content']/p//text()").extract()) + \
            "".join(response.selector.xpath("//div[@id='content']/div[not(@class)]//text()").extract())

        new_urls = response.selector.xpath("//div[@class='summary']/p/a/@href").extract()
        new_urls.extend(response.selector.xpath("//div[@id='content']//a/@href").extract())

        item = HudongbaikeItem()
        item['title'] = title
        item['abstract'] = abstract
        item['content'] = content
        item['url'] = response.url
        item['item_id'] = self.item_id
        self.item_id += 1
        if not(self.item_id % 10):
            print self.item_id
        yield item

        for url in new_urls:
            if "www.baike.com/wiki" in url:
                yield scrapy.Request(url, callback=self.parse)

    def close(self, reason):
        f = open("./last_id.txt","w")
        f.write(str(self.item_id))