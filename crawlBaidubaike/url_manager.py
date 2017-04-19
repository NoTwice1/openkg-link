#coding utf-8
class UrlManager(object):
    def __init__(self):
        self.new_url_list = set()
        self.old_url_list = set()
    def add_new_url(self, url):
        # pass
        if url is None:
            return
        if url not in self.new_url_list and url not in self.old_url_list:
            self.new_url_list.add(url)

    def has_new_url(self):
        return len(self.new_url_list) != 0

    def get_new_url(self):
        url = self.new_url_list.pop()
        self.old_url_list.add(url)
        return url

    def add_new_urls(self, urls):
        if urls is None or len(urls) == 0:
            return
        for url in urls:
            self.add_new_url(url)

    def _save_url(self):
        f = open('new_url_list','w')
        for url in self.new_url_list:
            f.write(url)
        f.close()

    def save_urls(self):
