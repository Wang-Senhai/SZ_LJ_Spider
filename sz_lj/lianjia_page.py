from bs4 import BeautifulSoup
import requests
import time
import os
import re
import math
import csv
import pymysql

url = 'https://sz.lianjia.com/zufang/SZ2286347245731725312.html'
list_base_url = 'https://sz.lianjia.com'
base_url = 'https://sz.lianjia.com/zufang/pagedirectionrent_price/#contentList'
pattern = re.compile('<div.*?oneline">(.*?)</li>.*?oneline">(.*?)</li>oneline">(.*?)</li>oneline">(.*?)</li>oneline">(.*?)</li>oneline">(.*?)</li>oneline">(.*?)</li>oneline">(.*?)</li>oneline">(.*?)</li>oneline">(.*?)</li>oneline">(.*?)</li>.*')
headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': 'TY_SESSION_ID=4161343e-3927-4100-9533-5adb529f3e0c; lianjia_uuid=6ed7df6c-ebe5-4568-a5bf-2c21e546ca39; Hm_lvt_9152f8221cb6243a53c83b956842be8a=1540622390; _smt_uid=5bd40836.2c7e9597; UM_distinctid=166b440154b306-06b5d47fe75004-47e1039-100200-166b440154d134; _jzqc=1; _jzqy=1.1540622391.1540622391.1.jzqsr=baidu|jzqct=%E9%93%BE%E5%AE%B6%E7%BD%91%E5%8C%97%E4%BA%AC.-; _jzqckmp=1; all-lj=3d8def84426f51ac8062bdea518a8717; _qzjc=1; CNZZDATA1254525948=667635047-1540617091-%7C1540622375; lianjia_ssid=f4217035-ac89-47ee-8799-142aaa2dedf2; _jzqa=1.4383878552266910700.1540622391.1540622391.1540624781.2; select_city=440300; CNZZDATA1255633284=1849769130-1540619624-%7C1540625025; CNZZDATA1255604082=1644370051-1540617480-%7C1540622360; Hm_lpvt_9152f8221cb6243a53c83b956842be8a=1540625223; _qzja=1.1426462676.1540622394159.1540622394159.1540625024479.1540625074932.1540625223403.0.0.0.17.2; _qzjb=1.1540625024479.3.0.0.0; _qzjto=17.2.0; _jzqb=1.4.10.1540624781.1; CNZZDATA1255849469=36534003-1540619700-%7C1540625224',
            'Host': 'sz.lianjia.com',
            'Referer': 'https://sz.lianjia.com/zufang',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
        }
house_head = {'url': '房源地址', 'district': '行政区', 'position': '位置', 
            'title': '标题', 'rent': '租金', 'fangshi': '租赁方式', 
            'huxing': '户型', 'mianji': '面积', 'chaoxiang': '朝向', 
            'img_list': '图片列表', 'fabu': '发布时间', 'ruzhu': '入住时间', 
            'zuqi': '租期', 'kanfang': '看房时间', 'louceng': '楼层', 'dianti': '电梯', 
            'chewei': '车位', 'shui': '用水', 'dian': '用电', 'ranqi': '燃气', 'cainuan': '采暖', 
            'tags': '标签', 'station': 'station', 
            'latitude': 'latitude', 'longtitude': 'longtitude'}
rent_price = {'rp1':'1k','rp2':'1.5k','rp3':'2k','rp4':'3k','rp5':'5k','rp6':'8k','rp7':'8k+'}
direction = {'f100500000001':'East','f100500000003':'South',
             'f100500000005':'West','f100500000007':'North',
             'f100500000002':'South-East','f100500000004':'South-West',
             'f100500000006':'Nouth-West','f100500000008':'Nouth-East',}
# db = pymysql.connect(host='localhost',user='root',password='wsh960126',port=3306,db='proxypool')
# cursor = db.cursor()
# cursor.execute("SELECT * FROM proxypool")
# result = cursor.fetchone()
# ip = result[1]
# port = result[2]

#主函数，如果任务列表文件list.csv不存在，则重新开始，如果已存在，则继续下载
def main():
    path = 'CSV/list.csv'
    if not os.path.exists(path):
        print('原文件不存在，正在重新获取...')
        pages = write_page_url()
        start(pages)
    else:
        print('文件已存在')
        pages = read_from_file(path)#从文件中读取
        print('已读取')
        start(pages)

#初始化爬虫任务，下载搜索条件对应的url和页数
def write_page_url():
    pages = get_page_numbers()
    write_csv_list(pages,'list')
    print('已保存搜索信息')
    return pages

#将获取的任务列表存在csv文件
def write_csv_list(result,name = 'sz'):
    head = []
    try:
        for key in result[0]:
            head.append(key)
    except:
        return 0
    with open('CSV/'+name+'.csv', 'a') as f: 
        writer = csv.DictWriter(f, fieldnames = head)
        writer.writeheader
        for dic in result:
            writer.writerow(dic)
        f.close()

#如果文件已存在，从文件中读出下载任务列表
def read_from_file(path):
    with open(path, 'r') as f: 
        csv_read = csv.reader(f)
        pages = []
        for line in csv_read:
            single = {}
            single['url'] = line[0]
            single['page'] = line[1]
            single['rp'] = line[2]
            single['direct'] = line[3]
            single['house_num'] = line[4]
            pages.append(single)
    return pages

#开始下载
def start(pages):
    for item in pages:#对每种搜索条件进行下载
        print('正在获取'+item['rp']+'价格下，房子朝向'+item['direct']+'方向的房源信息')
        print('共'+item['house_num']+'个房源满足此条件')
        path_name = item['rp']+'_'+item['direct']
        if not os.path.isdir('CSV/'+path_name):
            os.makedirs('CSV/'+path_name)
        for i in range(int(item['page'])):  #对不同页进行下载
            file_name = path_name+'/page_'+str(i+1)
            path = 'CSV/'+file_name+'.csv'
            if os.path.exists(path):
                print('该文件已存在！尝试下一页！')
                i+=1
                continue
            else:
                j = 1
                house_details = []#准备结果list
                url = get_page_url(item['url'],i)#获取当前搜索条件的第i页链接
                house_list = get_list_from_page(url)#获取第i页的房源链接列表
                for single in house_list:
                    print('正在获取第%d页，第%d个房源'%(i+1,j))
                    house = get_detail(single['url'])#下载每个房源的详情
                    house_info = single.copy()#更新原有字典
                    house_info.update(house)
                    house_details.append(house_info)
                    j+=1
                write_csv([house_head],name=file_name)#写入表头
                write_csv(house_details,name=file_name)#每一页储存一次，每个搜索方式存一个文件夹，每一页存一个文件
                i+=1#记得翻页
                print('已保存'+item['rp']+'价格下，房子朝向'+item['direct']+'方向的第'+str(i)+'页房源信息')
                print('还有%d页需要获取，已完成当前搜索条件的%.2f%%!'%(int(item['page'])-i,(i/int(item['page'])*100)))

#输入房源地址获取房源具体信息，返回字典
def get_detail(url):
    house = {}
    print('获取服务器响应...')
    try:
        response = requests.get(url,headers=headers)
    except requests.exceptions.RequestException as e:
        print('请求异常...')
        print('3秒后重试')
        time.sleep(3)
        try:
            response = requests.get(url,headers=headers)
        except requests.exceptions.RequestException as e:
            print('重试失败，跳过此页')
            return {}
    if response.status_code != 200:
        print('请求异常...')
        print('3秒后重试')
        time.sleep(3)
        response = requests.get(url,headers=headers)
        if response.status_code != 200:
            print('重试失败，跳过此页')
            return {}
    html = response.text
    soup = BeautifulSoup(html,features="lxml")
    try:
        house['title'] = soup.find('p',class_="content__title").get_text()
        print('已获取标题...')
    except AttributeError as e:
        try:
            house['title'] = soup.find('p', class_="content__aside--title").find('span').get_text()
            print('已获取标题...')
        except AttributeError as e:
            print('未发现房源标题！')
    try:
        house['rent'] = soup.find('p',class_="content__aside--title").get_text()
        print('已获取房租...')
    except AttributeError as e:
        try:
            rent = soup.find('p', class_="content__aside").find_all('span')
            house['rent'] = rent[1].get_text()
            print('已获取房租...')
        except AttributeError as e:
            print('未发现房租信息！')
    try:
        features = soup.find('p', class_="content__article__table").find_all('span')
        house['fangshi'] = features[0].get_text()
        house['huxing'] = features[1].get_text()
        house['mianji'] = features[2].get_text()
        house['chaoxiang'] = features[3].get_text()
        print('已获取户型...')
    except AttributeError as e:
        print('未发现户型信息！')
    img_list = []
    house['img_list'] = img_list
    try:
        img = soup.find('ul', class_="content__article__slide__wrapper").find_all('img')
        for item in img:
            img_list.append(item.get('src'))
        print('已获取图片...')
    except AttributeError as e:
        print('未发现图片！')
    try:
        item = soup.find('div', class_="content__article__info").find_all('li')
        house['fabu'] = item[1].get_text().strip()[3:]
        house['ruzhu'] = item[2].get_text().strip()[3:]
        house['zuqi'] = item[4].get_text().strip()[3:]
        house['kanfang'] = item[5].get_text().strip()[3:]
        house['louceng'] = item[7].get_text().strip()[3:]
        house['dianti'] = item[8].get_text().strip()[3:]
        house['chewei'] = item[10].get_text().strip()[3:]
        house['shui'] = item[11].get_text().strip()[3:]
        house['dian'] = item[13].get_text().strip()[3:]
        house['ranqi'] = item[14].get_text().strip()[3:]
        house['cainuan'] = item[16].get_text().strip()[3:]
        print('已获取基本信息...')
    except AttributeError as e:
        print('未发现基本信息！')
    tags = []
    try:
        tag = soup.find('p', class_="content__aside--tags").find_all('i')
        for item in tag:
            tags.append(item.get_text())
        house['tags'] = tags
        print('已获取标签...')
    except AttributeError as e:
        print('未发现标签信息！')
    try:
        distance = soup.find('div', class_="content__article__info4").find_all('li')
        i = 0
        station = []
        while i < len(distance):
            pair = distance[i].find_all('span')
            station.append(pair[0].get_text())
            station.append(pair[1].get_text())
            i+=1
        house['station'] = station
        print('已获取周边地铁信息...')
    except AttributeError as e:
        print('未发现地铁位置信息！')
    try:
        house['latitude'] = str(re.search("latitude: '(.*?)'",html,re.S)[0])[11:-1]
        house['longtitude'] = str(re.search("longitude: '(.*?)'",html,re.S)[0])[12:-1]
        print('已获取经纬度...')
        print('已获取本房源全部信息')
    except AttributeError as e:
        print('未发现地理位置信息！')
    print('已获取本房源全部信息')
    print('---------------------------')
    return house

#输入某页搜索结果，返回该页面所有房源的直接地址，行政位置
def get_list_from_page(url):
    list_from_page = []
    try:
        res = requests.get(url,headers=headers)
    except requests.exceptions.RequestException as e:
        print('请求异常...')
        print('3秒后重试')
        time.sleep(3)
        try:
            res = requests.get(url,headers=headers)
        except requests.exceptions.RequestException as e:
            print('重试失败，跳过此页')
            return []
    if res.status_code != 200:
        print('请求异常...')
        print('3秒后重试')
        time.sleep(3)
        res = requests.get(url,headers=headers)
        if res.status_code != 200:
            print('重试失败，跳过此页')
            return []
    html = res.text
    soup=BeautifulSoup(html,features="lxml")
    items = soup.find_all('div',class_='content__list--item--main')
    for item in items:
        house = {}
        house['url'] = list_base_url + item.find('a').get('href')
        location = item.find('p', class_="content__list--item--des").find_all('a')
        try:
            house['district'] = location[0].get_text()
            house['position'] = location[1].get_text()
        except:
            #print('无法获取行政区位置信息...')
            pass
        list_from_page.append(house)
    return list_from_page

#将房源具体信息列表内的字典写入csv文件
def write_csv(result,name = 'sz'):
    head = []
    try:
        for key in house_head:
            head.append(key)
    except:
        return 0
    with open('CSV/'+name+'.csv', 'a') as f: 
        writer = csv.DictWriter(f, fieldnames = head)
        writer.writeheader
        for dic in result:
            writer.writerow(dic)
        f.close()


#获取某查询条件下的页面总数，用于迭代页面
def get_total_page(url):
    try:
        res = requests.get(url,headers=headers)
    except requests.exceptions.RequestException as e:
        print('请求异常...')
        print('3秒后重试')
        time.sleep(3)
        try:
            res = requests.get(url,headers=headers)
        except requests.exceptions.RequestException as e:
            print('重试失败，跳过此页')
            return 0
    if res.status_code != 200:
        print('请求异常...')
        print('3秒后重试')
        time.sleep(3)
        res = requests.get(url,headers=headers)
        if res.status_code != 200:
            print('重试失败，跳过此页')
            return 0
    html = res.text
    soup=BeautifulSoup(html,features="lxml")
    number = soup.find('p',class_="content__title").find('span').get_text()
    page = math.ceil(int(number)/30)
    return page,number

#将房源页面信息汇总，存放在字典中，组成list返回
def get_page_numbers():
    all_page = []
    for rp in rent_price:
        price = rent_price[rp]
        single = {}
        for direct in direction:
            direct_value = direction[direct]
            url = base_url.replace('rent_price',rp)
            url = url.replace('direction',direct)
            url = url.replace('page','pg1')
            page,num = get_total_page(url)
            single['url'] = url
            single['page'] = page
            single['rp'] = price
            single['direct'] = direct_value
            single['house_num'] = num
            print('已获取'+price+'价格下，房子朝向'+direct_value+'方向的房源页数')
            input_single = single.copy()
            all_page.append(input_single)
    return all_page

#获取单个搜索条件下，第page页的url
def get_page_url(url,page):
    n = str(page)
    page = 'pg'+n
    url = url.replace('pg1',page)
    return url

# def get_response(url):
#     global ip,port
#     proxy = ip+':'+port
#     proxies = {
#     'http':'http://'+proxy,
#     'https':'https://'+proxy
#     }
#     try:
#         response = requests.get(url,headers=headers,proxies=proxies)
#     except requests.exceptions.RequestException as e:
#         ip,port = get_next_proxy()
#         try:
#             response = requests.get(url,headers=headers,proxies=proxies)
#         except requests.exceptions.RequestException as e:
#             return None
#     return response

# def get_next_proxy():
#     result = cursor.fetchone()
#     ip = result[1]
#     port = result[2]
#     return ip,port


if __name__ == '__main__':
    main()