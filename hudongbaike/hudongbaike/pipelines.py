# -*- coding: utf-8 -*-
import simplejson as json
import codecs


class HudongbaikePipeline(object):
    def __init__(self):
        # self.result_file = codecs.open("/mnt/hudong.json", 'a', 'utf-8')
        self.result_file = codecs.open("./hudong.json", 'a', 'utf-8')

    def process_item(self, item, spider):
        line = json.dumps(dict(item), ensure_ascii=False) + '\n'
        self.result_file.write(line)

        return item