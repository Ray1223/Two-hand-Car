from Redisdb import Redisdb, mySQL_project
import pymysql
import requests.exceptions
import sys
from bs4 import BeautifulSoup
import time
import redis

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def push_to_redis(links):
    for link in links:
        que.rpush('ucar_url', link)

def get_database_data():
    count = 0
    while count < 50:
        try:
            conn = pymysql.connect(host=mySQL_project.IP, port=3306,
                                   user='team1', passwd=mySQL_project.passwd, db='team1', charset='utf8')
            break
        except pymysql.OperationalError:
            count += 1
            if count == 50:
                logger.error('at data getting mysql server cannot connect')
    #     cur.set_character_set('utf8')
    c = conn.cursor()
    existing_data = []
    sql = "select url from {} where source ='{}' and (deal != '1' or deal is null)"
    c.execute(sql.format(mySQL_project.backup_table, 'UCar'))
    for row in c:
        existing_data.append(row)
    conn.commit()
    conn.close()
    return existing_data

def check_data_in_sql(url):
    if url in existing_data:
        return True
    else:
        return False

def connect_until_success(url):
    header = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, sdch, br",
    "Accept-Language": "en-US,en;q=0.8",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Cookie": "__asc=4add720515c7ed2e9a6d0297b1e; __auc=4add720515c7ed2e9a6d0297b1e; _ga=GA1.3.1702431505.1496776371; _gid=GA1.3.1258356823.1496776371; __gads=ID=e96348f54d1665de:T=1496776371:S=ALNI_MYKCtYxVc-kc1yxXE2dcazT7v_aRA",
    "Host": "usedcar.u-car.com.tw",
    "Upgrade-Insecure-Requests": "1",
    "ser-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36"
    }
    count = 0
    while count < 50:
        try:
            res = requests.post(url, headers = header, timeout = 2)
            break
        except requests.exceptions.ReadTimeout:
            count += 1
            if count == 50:
                logger.info('got event: {} cannot connect'.format(url))
            time.sleep(count * 0.1)
        except requests.exceptions.ConnectionError:
            count += 1
            if count == 50:
                logger.info('got event: {} cannot connect'.format(url))
            time.sleep(count * 0.1)
    return res

def UCarPrice_Crawler(pageNo):
    host = "https://usedcar.u-car.com.tw/"
    dirLinks = []
    brandList = ['toyota', 'mercedes-benz' , 'mazda', 'bmw', 'lexus', 'volkswagen', 'subaru', 'suzuki', 'nissan', 'audi',
               'honda', 'volvo', 'porsche', 'ford', 'mitsubishi']
    for page in range(1, pageNo):
        progress = int((page/pageNo)*100)
        printstring = ('total has {} pages, now is at page {}, which is {}% over'.format(pageNo, page+1, progress))
        sys.stdout.write('\r' + printstring )
        try:
            carLink = host + "index.aspx?page={}&during=1".format(page)
            res = connect_until_success(carLink)
            soup = BeautifulSoup(res.text, 'lxml')
            partLinks = soup.select(".title > .clean_a_tyle")
            for partLink in partLinks:
                brand = partLink.text.split()[0].lower()
                if brand in brandList:
                    if check_data_in_sql(host + partLink['href']):
                        pass
                    else:
                        dirLinks.append(host + partLink['href'])
            if page% 10 ==0:
                push_to_redis(dirLinks)
                dirLinks = []

        except Exception as e:
            logger.error('at page {} error occur Exception: {}'.format(page, e))

def main():
    global existing_data
    existing_data = get_database_data()
    host = "https://usedcar.u-car.com.tw/"
    global que
    que = redis.StrictRedis(host=Redisdb.host, port=Redisdb.port, db=0, password=Redisdb.password)
    que.delete('ucar_url')

    res = connect_until_success(host)
    soup = BeautifulSoup(res.text, 'lxml')
    totalpage = int(soup.select_one('li.arrow_right_2 > a')['href'].split("page=")[1].split("&during=1")[0])

    UCarPrice_Crawler(totalpage)
    end = ['end'] * 1000
    push_to_redis(end)
    print('\n' + 'crawling finished')


if __name__ == "__main__":
    main()