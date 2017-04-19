#coding utf-8
import html_downloader
import html_outputer
import html_parser
import url_manager
import time
# import urllib
import urllib2

BAIDUBAIKE_URL_BASE = 'http://baike.baidu.com/item/'
CRAWL_COUNT = 10000
class SpiderMain(object):
    def __init__(self):
        self.downloader = html_downloader.HtmlDownloader()
        self.parser = html_parser.HtmlParser()
        self.urls = url_manager.UrlManager()
        self.outputer = html_outputer.HtmlOutputer()
    def craw(self, root_url):
        count = 1
        self.urls.add_new_url(root_url)
        while self.urls.has_new_url():
            try:
                url = self.urls.get_new_url()
                print 'craw %d : %s'%(count,url)
                html_content = self.downloader.download(url)
                urls,data = self.parser.parse(url,html_content)
                self.urls.add_new_urls(urls)
                self.outputer.collect_data(data)
                count+=1
                if( (count+1) % 1000 == 0):
                    time.sleep(0.2)
                # if(count > CRAWL_COUNT):
                #     break
            except Exception as e:
                print("crawl failed")
                print e

        self.outputer.output_html()

    def crawlByEntityName(self):
        file_path = 'D:\Entity linkage\cndbpedia\cndbpediaDumpEntityName.txt'
        f = open(file_path,'r')
        for line in f:
            url = BAIDUBAIKE_URL_BASE + line.decode('utf-8')
            # crawUrl(url)
            html_content = self.downloader.download(url)
            urls,data = self.parser.parse(url,html_content)
            self.outputer.collect_data(data)
        f.close()

if __name__ == '__main__':
    root_url = "http://baike.baidu.com/item/python"
    obj_spider = SpiderMain()
    obj_spider.craw(root_url)