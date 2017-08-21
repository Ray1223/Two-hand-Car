from dbconfig import Redisdb, mySQL_project
import requests
import requests.exceptions
from bs4 import BeautifulSoup
from datetime import datetime
import pymysql
import time
import redis
import sys

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def connect_until_success(url):
    header = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, sdch, br",
        "Accept-Language": "zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Cookie": "__gads=ID=6dd06a6928bab430:T=1496776174:S=ALNI_MZw1RvLJ_ezD18iQcnLFUA0LsYbNQ; __RequestVerificationToken=oMBYkFk0gEocJ5npv8uUasjwq3v5YPCz5DB41DTuYdTP5P5feuleW8a7zBNYlUdZPxzPOBn4RRaktD4xSj1i-oMxRpg1;_gat=1; __asc=ec81b66215c834b6738ccfdfa0b; __auc=b3e83d1315c7ecf1e1aed870d7a; _ga=GA1.3.231933620.1496776122; _gid=GA1.3.1394749181.1496776122",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }

    count = 0
    while count < 50:
        try:
            res = requests.post(url, headers=header, timeout=3)
            while res.status_code == 502 or res.status_code == 403 or ('UCAR二手車' not in res.text):
                #                 proxies = gen_proxies()
                res = requests.post(url, headers=header, timeout=3)
                count += 1
                if count == 50:
                    logger.error('got event: {} cannot connect'.format(url))
                    break
            break
        except requests.exceptions.ReadTimeout:
            count += 1
            if count == 50:
                logger.error('got event: {} cannot connect'.format(url))
            time.sleep(count * 0.1)
        except requests.exceptions.ConnectionError:
            count += 1
            if count == 50:
                logger.error('got event: {} cannot connect'.format(url))
            time.sleep(count * 0.1)
        except (requests.exceptions.ProxyError, ConnectionRefusedError) as e:
            count += 1
            if count == 50:
                logger.error('got event: {} cannot connect'.format(url))
            time.sleep(count * 0.1)
    return res

def get_content(url):
    res = connect_until_success(url)
    soup = BeautifulSoup(res.text, 'lxml')
    if int(soup.select_one(".object_product_year").text.split('出廠')[0])>2007:
        modelWd = []
    #     try:
        car = {}
        brandsoup = soup.select_one(".object_product_title").text.lower().split()
        modelsoup = soup.select_one(".object_product_info").text.split()
        infosoup = soup.select(".gray_tab_info")
        #title
        car['title'] = soup.select_one(".object_product_title").text.replace('\xa0', '')
        #brand
        car['brand'] = brandsoup[0]
        #model
        """It's difficult to split the model. So, the column is  sometimes not clean. """
        if len(brandsoup) >= 2:
            for i in range(1, len(brandsoup)):
                modelWd.append(brandsoup[i])
            car['model'] = " ".join(modelWd)

        elif len(modelsoup) >=2:

            car['model'] = soup.select_one(".object_product_info").text.lower()

        else:
            car['model'] = None


        #source
        car['source'] = "UCar"
        #url
        car['url'] = url

        #cc
        if (len(soup.select_one(".object_product_cc").text) != None):
            car['cc'] = int(soup.select_one(".object_product_cc").text.split('c.c.')[0].replace(",", ""))
        else:
            car['cc'] = 0

        #equip
        equipWd = [equip.text for equip in soup.select('div.info_all > div.info_spec_s')]
        car['equip'] = "|".join(equipWd)

        aaa = car['equip'].split("|")
        before = ['安全氣囊','停車雷達系統','Keyless感應門鎖','HID氣體放電頭燈','天窗','衛星導航設備','ABS防鎖死煞車系統']
        after = ['安全氣囊','倒車雷達','keyless免鑰系統','HID頭燈','天窗','衛星導航','ABS']
        aaa = "|".join(set(aaa).intersection(before))
        data_dict = list(zip(before,after))
        for each_equip in data_dict:
            aaa = aaa.replace(each_equip[0],each_equip[1])
        car['equip'] = aaa

        #years
        car['years'] = int(soup.select_one(".object_product_year").text.split('出廠')[0])
        #location
        car['location'] = None
        #doors
        car['doors'] = None
        #posttime
        strtime = soup.select(".number")[0].text
        timeArray = datetime.strptime(strtime, "%Y/%m/%d %H:%M")
        car['posttime'] = float(timeArray.timestamp())

        #content
        car['content'] = soup.select_one(".displayl-block.align-center").text.replace('\n', '').replace('\xa0','').replace('r','')
        #certificate
        car['certificate'] = None
        # soldout
        """soldout type is int ,there is no boolean type in sqlite3"""
        car['soldout'] = 0

        # price
        if (soup.select_one("p.price").text != "已收訂" and soup.select_one("p.price").text != "面議"):
            car['price'] = round(int(soup.select_one("p.price").text.replace(",", "")) / 10000)
        else:
            car['soldout'] = 1
            car['price'] = 0

        #offtime
        car['offtime'] = None


        if len(infosoup)==5:
            # type
            car['type'] = infosoup[0].text
            # gasoline $
            car['gasoline'] = infosoup[1].text.replace(" ", "")
            # transmission $
            car['transmission'] = infosoup[2].text.replace(" ", "")
            # color $
            car['color'] = infosoup[3].text
            # mileage $
            car['mileage'] = int(infosoup[4].text.replace("公里", "").replace(",", ""))

        if len(infosoup) == 4:
            # type
            car['type'] = None
            # gasoline $
            car['gasoline'] = infosoup[0].text.replace(" ", "")
            # transmission $
            car['transmission'] = infosoup[1].text.replace(" ", "")
            # color $
            car['color'] = infosoup[2].text
            # mileage $
            car['mileage'] = int(infosoup[3].text.replace("公里", "").replace(",", ""))

    #     except Exception as e:
    #         print("There is get_UCar_Item exception as below :{} URL: {}".format(e, url))
        return car
    else:
        return None


def add_to_mysql(car):
    if car is not None:
        source = car['source']
        url = car['url']
        title = car['title']
        brand = car['brand'].upper().replace('VM','volkswagen').replace('M-BENZ','BENZ').replace('MERCEDES-BENZ','BENZ')
        model = car['model']
        doors = car['doors']
        color = car['color']
        gasoline = car['gasoline'].replace('油電混合', '油電')
        cc = car['cc']
        transmission = car['transmission']
        equip = car['equip']
        mileage = car['mileage']
        years = car['years']
        location = car['location']
        posttime = datetime.fromtimestamp(float(car['posttime']))
        price = car['price']
        certificate = car['certificate']
        #         content = car['content']
        offTime = (car['offtime'])
        deal = car['soldout']
        #         type = car['type']
        query = (source, url, title, brand, model, doors, color, cc, transmission, equip,
                 mileage, years, location, posttime, price, certificate, gasoline, deal, offTime,)
        while True:
            try:
                conn = pymysql.connect(host=mySQL_project.IP, port=3306,
                                       user='team1', passwd=mySQL_project.passwd, db='team1', charset='utf8')
                break
            except pymysql.OperationalError:
                pass

        c = conn.cursor()
        try:
            command = 'INSERT INTO {} VALUES ' \
                      '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'.format(
                mySQL_project.temp_table)
            c.execute(command, query)
            command = 'INSERT INTO {} VALUES ' \
                      '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'.format(
                mySQL_project.backup_table)
            c.execute(command, query)
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error('at data {} mysql server cannot connect: {}'.format(url, e))
            conn.rollback()
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

def check_duplicates(url):
    if url in existing_data:
        return True
    else:
        return False
def main():
    starttime = time.time()
    alreadyCrawled = 0
    global existing_data
    existing_data = get_database_data()
    que = redis.StrictRedis(host=Redisdb.host, port=Redisdb.port, db=0, password=Redisdb.password)
    que.delete('ucar_failed')
    while True:
        count = 3
        url = que.blpop('ucar_url')[1].decode('utf8')
        if url == 'end':
            break
        printstring = ('*****we have crawled ' + str(alreadyCrawled) + ' pages *****')
        sys.stdout.write('\r' + printstring)
        while count:
            try:
                if not check_duplicates(url):
                    content_dict = get_content(url)
                    add_to_mysql(content_dict)
                    existing_data.append(url)
                    alreadyCrawled += 1
                    break
                else:
                    #                 print('same car exist pass')
                    break
            except IndexError as e:
                print(e)
                count -= 1
            endtime = time.time()
            if endtime - starttime >= 280:
                break
            else:
                pass
            if count == 0:
                que.lpush('ucar_failed', url)

    print('\n' + 'crawling finished!')


if __name__=='__main__':
    main()