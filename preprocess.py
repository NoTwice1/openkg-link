#coding: utf-8
import urllib
import gzip
import os
from collections import defaultdict
from matplotlib import pyplot as plt
import csv
import glob

from pylab import mpl
mpl.rcParams['font.sans-serif'] = ['SimHei']

#zhishi.me源数据的存储路径
zhishime_data_dir = 'F:/KBs/zhishime/'
#复旦百科源数据的存储路径
cndb_path = u"F:/KBs/cndbpediaDump/cndbpediaDump.txt"

#三个源预处理的结果的存放路径
preprocess_dir = "D:/record_linkage/data/"

hudongbaike_dir = preprocess_dir + "hudongbaike/"
baidubaike_dir = preprocess_dir + "baidubaike/"
zhwiki_dir = preprocess_dir + "zhwiki/"
sameas_dir = preprocess_dir + "sameas/"


def encode_nt_file(file_path, new_file_dir, sub, pred, obj):
    '''
    读取nt格式的文件，将里面的urlencode或者unicode编码的中文转化为utf-8,再写入新的nt文件
    可以处理gz压缩文件和常规文件，但是不能处理rar压缩文件
    :param: sub, pred, obj: 指定原数据文件中一行三个部分的编码格式
            'uni': unicode;  'url': urlencode
    '''
    new_file_dir = new_file_dir + "/" if new_file_dir[-1] != '/' else new_file_dir
    new_file_path = new_file_dir + os.path.basename(file_path).replace(".gz","")
    g = open(new_file_path,"w")
    funcs = {'url': lambda x: urllib.unquote(x), 
            'uni': lambda x: x.decode('unicode_escape').encode('utf-8')}
    
    is_gz = file_path.endswith(".gz")
    f = gzip.GzipFile(file_path) if is_gz else open(file_path)
    count = 0
    for line in f:
        parts = line.split(' ')
        subject = funcs[sub](parts[0])
        predicate = funcs[pred](parts[1])
        if len(parts) > 3:
            objectt = funcs[obj](' '.join(parts[2:-1]))
        else:
            objectt = funcs[obj](parts[2:-1])
        new_line = ' '.join([subject, predicate, objectt] + [parts[-1]])
        g.write(new_line)

        count += 1
        if not (count % 50000):
            print u"已处理: ", count, u" 行"
    f.close()
    g.close()

def get_URI_base(URI, separator):
    '''
    获取URI末尾的有用部分，如中文内容，属性名称等
    :param separator: 分隔符 / 或 # 或 None
    '''    
    URI = URI.strip()
    if separator == '/' or separator == "#":
        base = URI[URI.rfind(separator)+1:-1]
    else:
        base = URI
    if base.endswith("@zh"):
        base = base[:-3]    
    if base[0] == '"':
        base = base[1:-1]
    return base

def nt_to_triple(file_path, new_file_dir, sub, pred, obj, separators, need_pred=True):
    '''
    从nt格式的文件转换到triple(\t分隔)格式,去掉http链接部分，提取出有效的中文部分
    :param sub, pred, obj:  nt格式文件里一个triple这三个部分是什么编码 url or unicode
    :param separators: [-,-,-], 以上三个部分在URI中有效部分的分隔符, '/' or '#' or None
    :param need_pred: 是否需要该文件里的属性和属性值，还是只需要实体
    ''' 
    if file_path.endswith('.rar'):
        file_path = file_path.replace('.rar', '.nt')
        if not os.path.exists(file_path):            
            print u'!!!!!!!!!!!!!!!!!!!!!!'
            print u'请手动解压rar文件， python的rar支持太坑了, 原地解压即可'
            print u'文件名', file_path.replace(".nt",'') + '.rar'
            print u'无法处理, 跳过该文件'
            print u'!!!!!!!!!!!!!!!!!!!!!!\n'
            return False
        else:
            print u'该文件已解压，直接处理解压后的文件'
            print u'文件名', file_path

    new_file_dir = new_file_dir + "/" if new_file_dir[-1] != '/' else new_file_dir
    new_file_path = new_file_dir + os.path.basename(file_path).replace(".nt","").replace('.gz','') + ".txt"
    if os.path.exists(new_file_path):
        print u'该文件已处理过，跳过'
        print u'文件名', file_path
        print 
        return True
    print u'正在处理文件: ', file_path

    funcs = {'url': lambda x: urllib.unquote(x).decode('utf-8'), 
            'uni': lambda x: x.decode('unicode_escape')}
        
    entities = {}

    is_gz = file_path.endswith('.gz')
    f = gzip.GzipFile(file_path) if is_gz else open(file_path)

    count = 0        
    for line in f:
        parts = line.split(' ')
        
        subject = funcs[sub](get_URI_base(parts[0], separators[0]))

        if need_pred:        
            predicate = funcs[pred](get_URI_base(parts[1], separators[1]))

            if len(parts) > 3:
                tmp = ' '.join(parts[2:-1])
            else:
                tmp = parts[2]

            try:
                objectt = funcs[obj](get_URI_base(tmp, separators[2]))
            except:
                continue          

        if subject not in entities:
            entities[subject] = {}

        if need_pred:
            if predicate not in entities[subject]:
                entities[subject][predicate] = [objectt]
            else:
                entities[subject][predicate].append(objectt)

        
        count += 1
        # if count == 1:
        #     print u'显示源文件第一行'
        #     print line
        #     print u'显示triple前10行: '
        # if count < 10:
        #     if need_pred:
        #         print subject,predicate,objectt
        #     else:
        #         print subject
        if not (count % 500000):
            print u"已处理: ", count, u" 行"

    print u'共 ', count, u'行'
    g = open(new_file_path,"wb")
    if need_pred:
        for en in entities:   
            for attr in entities[en]:
                line = en.encode('utf-8') + '\t' + attr.encode('utf-8')
                for val in entities[en][attr]:                                            
                    g.write(line + '\t' + val.encode('utf-8') +'\n')
    else:
        for en in entities:
            g.write(en.encode('utf-8') + '\n')

    f.close()
    g.close()

    return True

def count_cndb_attrs(file_path, first_n=20):
    '''
    统计cndbpedia中最常见的n个属性
    '''
    attrs = defaultdict(int)
    f = open(file_path)
    count = 0
    for line in f:
        try:
            attr = line.split("\t")[1].decode('utf-8')
            attrs[attr] += 1
        except:
            break

        count += 1
        if not (count % 500000):
            print u"已处理: ", count, u" 行"

    print u"属性总数目: ", len(attrs)
    sorted_attrs = sorted(attrs.iteritems(), key=lambda x:x[1], reverse=True)
    attr_names = map(lambda x:x[0], sorted_attrs[:first_n])
    attr_cnts = map(lambda x:x[1], sorted_attrs[:first_n])

    x_pos = range(len(attr_names))
    plt.bar(x_pos, attr_cnts, align='center')
    plt.ylabel(u'属性出现次数')
    plt.xticks(x_pos, attr_names)
    plt.title(u"前n个属性统计")
    plt.show()

def extract_hudongbaike():
    '''
    从hudongbaike数据中提取所有实体和部分有效的属性
    '''
    not_done_list = []
    hudong_dir = zhishime_data_dir + 'hudongbaike/'
    if not os.path.exists(hudong_dir):
        print u'目录不存在, 请确认目录没填错？？'
        return 

    file_path = unicode(hudong_dir + '3.0_hudongbaike_infobox_properties_zh.nt.gz','utf-8')
    res = nt_to_triple(file_path,hudongbaike_dir,'url','url','uni',['/','/',None])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(hudong_dir + '3.0_hudongbaike_article_categories_zh.rar','utf-8')    
    res = nt_to_triple(file_path,hudongbaike_dir,'url','url','url',['/','/','/'])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(hudong_dir + '3.0_hudongbaike_redirects_zh.nt.gz','utf-8')
    res = nt_to_triple(file_path,hudongbaike_dir,'url','url','url',['/','/','/'])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(hudong_dir + '3.0_hudongbaike_disambiguations_zh.nt.gz','utf-8')
    res = nt_to_triple(file_path,hudongbaike_dir,'url','url','url',['/','/','/'])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(hudong_dir + 'hudongbaike_instance_types_zh.rar','utf-8')
    res = nt_to_triple(file_path,hudongbaike_dir,'url','url','url',['/','#','/'])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(hudong_dir + '3.0_hudongbaike_abstracts_zh.nt.gz','utf-8')    
    res = nt_to_triple(file_path,hudongbaike_dir,'url','url','uni',['/','/',None], False) 
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(hudong_dir + '3.0_hudongbaike_article_links_zh.nt.gz','utf-8')    
    res = nt_to_triple(file_path,hudongbaike_dir,'url','url','url',['/','/','/'], False)   
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(hudong_dir + '3.0_hudongbaike_external_links_zh.nt.gz','utf-8')    
    res = nt_to_triple(file_path,hudongbaike_dir,'url','url','url',['/','/',None], False)
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(hudong_dir + '3.0_hudongbaike_related_pages_zh.nt.gz','utf-8')
    res = nt_to_triple(file_path,hudongbaike_dir,'url','url','url',['/','/','/'], False) 
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(hudong_dir + '3.0_hudongbaike_images_zh.nt.gz','utf-8')
    res = nt_to_triple(file_path,hudongbaike_dir,'url','url','url',['/','/',None], False) 
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(hudong_dir + '3.0_hudongbaike_internal_links_zh.nt.gz','utf-8')
    res = nt_to_triple(file_path,hudongbaike_dir,'url','url','url',['/','/','/'], False) 
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(hudong_dir + '3.0_hudongbaike_labels_zh.nt.gz','utf-8')
    res = nt_to_triple(file_path,hudongbaike_dir,'url','url','uni',['/','#',None]) 
    if not res:
        not_done_list.append(file_path)


    if not_done_list:
        print u'还有文件未处理, 请手动解压rar文件'
        print u'#########################'
        for file_name in not_done_list:
            print u"文件名: ", file_name
        print u'#########################'
        return False
    else:
        print u'全部文件处理完成'
    return True

def extract_baidubaike():
    '''
    从baidubaike数据中提取所有实体和部分有效的属性
    '''
    not_done_list = []
    baidu_dir = zhishime_data_dir + 'baidubaike/'
    if not os.path.exists(baidu_dir):
        print u'目录不存在, 请确认目录没填错？？'
        return 

    file_path = unicode(baidu_dir + '3.0_baidubaike_infobox_properties_zh.nt.gz','utf-8')
    res = nt_to_triple(file_path,baidubaike_dir,'url','url','uni',['/','/',None])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(baidu_dir + '3.0_baidubaike_article_categories_zh.rar','utf-8')    
    res = nt_to_triple(file_path,baidubaike_dir,'url','url','url',['/','/','/'])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(baidu_dir + '3.0_baidubaike_redirects_zh.nt.gz','utf-8')
    res = nt_to_triple(file_path,baidubaike_dir,'url','url','url',['/','/','/'])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(baidu_dir + '3.0_baidubaike_disambiguations_zh.nt.gz','utf-8')
    res = nt_to_triple(file_path,baidubaike_dir,'url','url','url',['/','/','/'])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(baidu_dir + 'baidubaike_instance_types_zh.rar','utf-8')
    res = nt_to_triple(file_path,baidubaike_dir,'url','url','url',['/','#','/'])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(baidu_dir + '3.0_baidubaike_abstracts_zh.nt.gz','utf-8')    
    res = nt_to_triple(file_path,baidubaike_dir,'url','url','uni',['/','/',None], False) 
    if not res:
        not_done_list.append(file_path)  

    file_path = unicode(baidu_dir + '3.0_baidubaike_external_links_zh.nt.gz','utf-8')    
    res = nt_to_triple(file_path,baidubaike_dir,'url','url','url',['/','/',None], False)
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(baidu_dir + '3.0_baidubaike_related_pages_zh.nt.gz','utf-8')
    res = nt_to_triple(file_path,baidubaike_dir,'url','url','url',['/','/','/'], False) 
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(baidu_dir + '3.0_baidubaike_images_zh.nt.gz','utf-8')
    res = nt_to_triple(file_path,baidubaike_dir,'url','url','url',['/','/',None], False) 
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(baidu_dir + '3.0_baidubaike_internal_links_zh.nt.gz','utf-8')
    res = nt_to_triple(file_path,baidubaike_dir,'url','url','url',['/','/','/'], False) 
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(baidu_dir + '3.0_baidubaike_labels_zh.nt.gz','utf-8')
    res = nt_to_triple(file_path,baidubaike_dir,'url','url','uni',['/','#',None]) 
    if not res:
        not_done_list.append(file_path)


    if not_done_list:
        print u'还有文件未处理, 请手动解压rar文件'
        print u'#########################'
        for file_name in not_done_list:
            print u"文件名: ", file_name
        print u'#########################'
        return False
    else:
        print u'全部文件处理完成'
    return True

def extract_zhwiki():
    '''
    从zhwiki数据中提取所有实体和部分有效的属性
    '''
    not_done_list = []
    zhwiki = zhishime_data_dir + 'zhwiki/'
    if not os.path.exists(zhwiki):
        print u'目录不存在, 请确认目录没填错？？'
        return 

    file_path = unicode(zhwiki + '2.0_zhwiki_infobox_properties_zh.rar','utf-8')
    res = nt_to_triple(file_path,zhwiki_dir,'url','uni','uni',['/','/',None])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(zhwiki + '2.0_zhwiki_article_categories_zh.rar','utf-8')    
    res = nt_to_triple(file_path,zhwiki_dir,'url','url','url',['/','/','/'])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(zhwiki + '2.0_zhwiki_redirects_zh.rar','utf-8')
    res = nt_to_triple(file_path,zhwiki_dir,'url','url','url',['/','/','/'])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(zhwiki + '2.0_zhwiki_disambiguations_zh.rar','utf-8')
    res = nt_to_triple(file_path,zhwiki_dir,'url','url','url',['/','/','/'])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(zhwiki + 'zhwiki_instance_types_zh.rar','utf-8')
    res = nt_to_triple(file_path,zhwiki_dir,'url','url','url',['/','#','/'])
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(zhwiki + '2.0_zhwiki_abstracts_zh.rar','utf-8')    
    res = nt_to_triple(file_path,zhwiki_dir,'url','url','uni',['/','/',None], False) 
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(zhwiki + '2.0_zhwiki_article_links_zh.rar','utf-8')    
    res = nt_to_triple(file_path,zhwiki_dir,'url','url','url',['/','/','/'], False)   
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(zhwiki + '2.0_zhwiki_external_links_zh.rar','utf-8')    
    res = nt_to_triple(file_path,zhwiki_dir,'url','url','url',['/','/',None], False)
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(zhwiki + '2.0_zhwiki_internal_links_zh.rar','utf-8')
    res = nt_to_triple(file_path,zhwiki_dir,'url','url','url',['/','/','/'], False) 
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(zhwiki + '2.0_zhwiki_labels_zh.rar','utf-8')
    res = nt_to_triple(file_path,zhwiki_dir,'url','url','uni',['/','#',None]) 
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(zhwiki + '2.0_zhwiki_aliases_zh.rar','utf-8')
    res = nt_to_triple(file_path,zhwiki_dir,'url','url','uni',['/','/',None]) 
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(zhwiki + '2.0_zhwiki_dbpedia_links_zh.rar','utf-8')
    res = nt_to_triple(file_path,zhwiki_dir,'url','url','url',['/','#','/'], False) 
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(zhwiki + '2.0_zhwiki_resource_ids_zh.rar','utf-8')
    res = nt_to_triple(file_path,zhwiki_dir,'url','url','url',['/','/',None], False)
    if not res:
        not_done_list.append(file_path)

    file_path = unicode(zhwiki + '2.0_zhwiki_revisions_zh.rar','utf-8')
    res = nt_to_triple(file_path,zhwiki_dir,'url','url','url',['/','/',None], False)
    if not res:
        not_done_list.append(file_path)


    if not_done_list:
        print u'还有文件未处理, 请手动解压rar文件'
        print u'#########################'
        for file_name in not_done_list:
            print u"文件名: ", file_name
        print u'#########################'
        return False
    else:
        print u'全部文件处理完成'
    return True

def extract_sameas():
    not_done_list = []
    sameas = zhishime_data_dir + 'sameAs/'
    files = ['2.9_zhwiki_hudongbaike_links_zh.rar','2.9_baidubaike_hudongbaike_links_zh.nt.gz',\
                '2.9_baidubaike_zhwiki_links_zh.nt.gz']

    for basename in files:
        file_path = unicode(sameas + basename, 'utf-8')
        res = nt_to_triple(file_path, sameas_dir, 'url','url','url',['/','#','/'], True)
        if not res:
            not_done_list.append(file_path)

    if not_done_list:
        print u'还有文件未处理, 请手动解压rar文件'
        print u'#########################'
        for file_name in not_done_list:
            print u"文件名: ", file_name
        print u'#########################'
        return False
    else:
        print u'全部文件处理完成'
    return True



def combine_triple_files(files, entities_file, attr_file):
    entities = {}
    attrs = defaultdict(int)
    for file_name in files:
        print u"读取文件", file_name
        count = 0
        has_attr = False
        f = open(file_name)        
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) == 1:
                has_attr = False
            else:
                has_attr = True

            if has_attr:
                try:
                    sub, pred, objj = parts
                except:
                    has_attr = False
                    print parts
            else:
                sub = parts[0]

            if sub not in entities:
                entities[sub] = {}

            if has_attr:
                if pred not in entities[sub]:
                    entities[sub][pred] = [objj]
                else:
                    entities[sub][pred].append(objj)

                attrs[pred] += 1

            count += 1
            if not (count % 1000000):
                print u"已读取 ", count, u' 行'
        print u"读取完毕"
        f.close()

    print u"写入文件 ", entities_file, " ..."
    f = open(entities_file,'w')
    for en in entities:
        if not len(entities[en]):
            f.write(en + '\n')
        else:
            for attr in entities[en]:                    
                for val in entities[en][attr]:
                    f.write(en + '\t' + attr + '\t' + val + '\n')
    f.close()

    print u'写入文件', attr_file, ' ...'
    f = open(attr_file, 'w')
    for attr in attrs:
        f.write(attr + '\t' + unicode(attrs[attr]).encode('utf-8') + '\n')
    f.close()

def combine_single_source(dir_name):
    '''
    合并单个源的所有单个预处理的结果文件
    结果：
    total.txt  tsv格式的实体，属性，属性值
    attr.txt  tsv格式的属性 出现次数
    '''
    files = glob.glob(dir_name + "*")
    combine_triple_files(files, dir_name+'total.txt',dir_name +'attr.txt')

def combine_three_source():
    files = [hudongbaike_dir+'total.txt',baidubaike_dir+'total.txt',zhwiki_dir+'total.txt']
    combine_triple_files(files, preprocess_dir+"total.txt",preprocess_dir+'attr.txt')

if __name__ == "__main__":
    # file_path = unicode('F:/KBs/zhishime/hudongbaike/3.0_hudongbaike_infobox_properties_zh.nt','utf-8')
    # encode_nt_file(file_path,hudongbaike_dir,'url','url','uni',is_gz=False)
    
    # count_cndb_attrs(cndb_path, 30)

    ######################
    extract_hudongbaike()
    # combine_single_source(hudongbaike_dir)

    # extract_baidubaike()
    # combine_single_source(baidubaike_dir)

    # extract_zhwiki()
    # combine_single_source(zhwiki_dir)

    # combine_three_source()

    # extract_sameas()

    # count_cndb_attrs(cndb_path)