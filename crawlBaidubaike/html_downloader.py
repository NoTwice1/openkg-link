#coding utf-8
import urllib2
import requests

class HtmlDownloader(object):
    def __init__(self):
        self.currentProxy = '222.186.161.132:3128'
        self.proxies = set()

        # f = open('proxies.txt','r')
        # for line in f:
        #     self.proxies.add(line.decode('utf-8'))
        # f.close()

    def change_proxy(self):
        self.currentProxy = self.proxies.pop()

    def download(self, url):
        if(url is None):
            return None

        r = None
        # while True:
        r = requests.get(url,proxies={'http':'http://'+self.currentProxy},timeout=2)

        # except Exception as e:
        #     print u'downloader'
        #     print e
        return r.text


