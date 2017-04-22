# -*- coding: utf-8 -*-
import json
import codecs


class HudongbaikePipeline(object):
    def __init__(self):
        self.result_file = codecs.open("./result.json", 'a', 'utf-8')

    def process_item(self, item, spider):
        self.last_id = item['item_id']

        line = json.dumps(dict(item), ensure_ascii=False) + '\n'
        self.result_file.write(line)

        return item