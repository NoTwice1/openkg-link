# -*- coding:utf-8 -*-

# 引入文件
import json
import codecs

class BaidubaikePipelines(object):

    # 该方法用于处理数据
    def __init__(self):
        self.file = None

    def process_item(self, item, spider):
        # 读取item中的数据
        line = json.dumps(dict(item), ensure_ascii=False) + "\n"
        # 写入文件
        self.file.write(line)

        # 返回item
        return item

    # 该方法在spider被开启时被调用。
    def open_spider(self, spider):
        self.file = codecs.open('baidubaikeItem.json', 'a',"utf-8")

    # 该方法在spider被关闭时被调用。
    def close_spider(self, spider):
        self.file.close()
