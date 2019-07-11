##########################################################################
#将csv文件写入到数据库中
#链接数据库，新建表格，id和url设置为双主键，id自动增长
#写入表头，获取csv文件信息，写入数据
#使用双主键中的url去重
##########################################################################

import pymysql
import csv

db = pymysql.connect(host = 'localhost', user = 'root', password = 'wsh960126', db = 'zufang')
cursor = db.cursor()
sql_add_column = 'ALTER TABLE clear_data ADD COLUMN {0} VARCHAR({1}) NULL'
sql_delete_colum = 'ALTER TABLE clear_data DROP COLUMN {}'
max_length = [56, 4, 5, 30, 51, 6, 7, 5, 13, 3347, 5, 10, 7, 7, 8, 4, 4, 4, 4, 4, 4, 71, 258, 18, 18]
keys = ['url', 'district', 'position', 'title', 'rent', 'fangshi', 'huxing', 'mianji', 
        'chaoxiang', 'img_list', 'fabu', 'ruzhu', 'zuqi', 'kanfang', 'louceng', 'dianti', 
        'chewei', 'shui', 'dian', 'ranqi', 'cainuan', 'tags', 'station', 'latitude', 'longtitude']
#######################创建数据表###########################################
create_table = '''
            CREATE TABLE clear_data(
            id BIGINT NOT NULL AUTO_INCREMENT,
            url VARCHAR(56) NOT NULL,
            PRIMARY KEY (id, url)); 
            '''
cursor.execute(create_table)

######################创建数据库表格的列######################################
for i in range(len(keys)):
    try:
        cursor.execute(sql_add_column.format(keys[i],max_length[i]))
    except pymysql.err.InternalError as e:
        print(e.args)
        continue
#######################获取数据格式##########################################
def insert(data):
    f = '"'
    base = "INSERT INTO clear_data (url, district, position, title, rent, fangshi, huxing, mianji, chaoxiang, img_list, fabu, ruzhu, zuqi, kanfang, louceng, dianti, chewei, shui, dian, ranqi, cainuan, tags, station, latitude, longtitude) VALUES ({});"
    #base = "INSERT INTO rent  VALUES ({});"
    for i in range(len(data)):
        f = f+data[i]+'","'
    f = f[:-2]
    return base.format(f)
#######################获取并添加数据##########################################
with open('/Users/luke_hai/Desktop/Python/spider/zufang/all_result.csv', 'r') as lf: 
    csv_read = csv.reader(lf)
    total = 33637
    cnt = 0
    for line in csv_read:
        print('Processing: %.2f%%, Number %d'%((cnt/total)*100,cnt), end='\r')
        line[4] = line[4][:5].replace('元','').replace('/','')
        cnt+=1
        sql = insert(line)
        try:
            cursor.execute(sql)
            db.commit()
        except:
            print()
            print(cnt)
            db.rollback()
print()
cursor.close()

