#coding:utf-8
from preprocess import cndb_path, hudongbaike_dir
from collections import defaultdict

#hudongbaike文件的路径
hudongbaike_data = hudongbaike_dir + 'total.txt'

#知识库文件的行数
cndb_lines = 87070270
hudongbaike_lines = 16306872

#一次读取的行数
batch_size = 10000000

#写入结果的文件名
w = open('results/link_by_entity_name.txt', 'w')

def get_file_lines(file_path):
    '''
    获取文件的行数
    '''
    print u'统计文件的行数 ', file_path
    f = open(file_path)
    cnt = 0
    for _ in f:
        cnt += 1
    print u'统计完成 ', cnt, u' 行'
    return cnt

#--------------------------------------------------------
# block function
def whole_entity_name(entity):
    '''
    entity的结构: (entity_name, {}), {}里是属性和属性值
    '''
    return entity[0]

def first_two_letter(entity):
    return entity[0][:2]

#--------------------------------------------------------
# similarity function
def same_name(e1, e2):
    '''
    e1, e2: 来自两个知识库的实体
    '''
    return e1[0] == e2[0]


#--------------------------------------------------------
# real work
def read_KB_part(KB_path, part_index, batch_lines=batch_size, attrs_map={}):
    '''
    读取一个知识库文件的一部分(文件太大)。 返回一个迭代器，每次返回一个entity

    KB_path: 知识图谱文件的路径
    part_index: 当前是该文件的第几部分
    batch_size: 一次读取多少行
    attrs_map: 属性名映射，将需要的属性读取出来，并映射属性名到别的名称
    '''
    print u'读取文件 ', KB_path, ' epoch ', part_index
    begin_line = part_index * batch_lines
    end_line = (part_index+1) * batch_lines

    f = open(KB_path)
    cnt = 0   #当前行数
    last = None   #上一行的实体名
    entity = ()

    for line in f:
        cnt += 1
        if cnt < begin_line:
            continue

        line = unicode(line,'utf-8')
        parts = line.strip().split('\t')
        if len(parts) == 1:
            sub = parts[0]
        else:
            sub, pred, objj = parts[0], parts[1], parts[2:]

        if last is None:
            last = sub
            entity = (sub, {})
        elif sub == last:
            if pred in attrs_map:
                entity[1][attrs_map[pred]] = objj
        else:            
            yield entity
            last = sub
            entity = (sub, {})
        
        if cnt > end_line:
            break

    print u'读取完毕'
    yield entity


def block_KB_part(KB_path, part_index, block_func):
    '''
    对一个知识库的一部分进行blocking
    返回一个dict, key是block_key, 值是该key下面的所有entities

    KB_path: 知识库文件的路径
    part_index: 当前是该知识库的第几部分
    block_func: blocking function
    '''

    blocks = defaultdict(list)

    print u'开始 blocking', KB_path, ' epoch ', part_index

    for entity in read_KB_part(KB_path, part_index):
        block_key = block_func(entity)
        blocks[block_key].append(entity)

    print u'完成 blocking', KB_path, ' epoch ', part_index
    print 

    return blocks    

def generate_entity_pair(block1, block2):
    '''
    将来自两个知识库的两个block里的entities两两配对
    '''
    for e1 in block1:
        for e2 in block2:
            yield [e1, e2]

def link_block(block1, block2, sim_func):
    '''
    将来自两个知识库的两个block里的entities进行实体链接

    sim_func: 两个实体是否可以链接
    '''
    for e1, e2 in generate_entity_pair(block1, block2):        
        if sim_func(e1, e2):
            w.write(e1[0].encode('utf-8')+'\t'+'sameAs'+'\t'+e2[0].encode('utf-8')+'\n')    

def link_KB(KB_path1, KB_path2):
    '''
    链接两个知识库，参数为两个知识库的路径
    '''

    # kb1_lines = get_file_lines(KB_path1)
    # kb2_lines = get_file_lines(KB_path2)

    epoch1 = int(cndb_lines / batch_size) + 1
    epoch2 = int(hudongbaike_lines / batch_size) + 1
    print "total epoch1: ", epoch1
    print 'total epoch2: ', epoch2

    for i in range(8, epoch1):

        blocks1 = block_KB_part(KB_path1, i, first_two_letter)

        for j in range(1, epoch2):
            print "epoch1 ", i
            print "epoch2 ", j

            
            blocks2 = block_KB_part(KB_path2, j, first_two_letter)
            
            print u'blocks1 数目', len(blocks1)
            print u'blocks2 数目', len(blocks2)

            for key1 in blocks1:
                if key1 in blocks2:                    
                    link_block(blocks1[key1], blocks2[key1], same_name)

if __name__ == '__main__':
    link_KB(cndb_path, hudongbaike_data)