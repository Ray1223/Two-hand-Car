def get_UCar_Item():
    modelWd = []
    try:
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
            car['model'] = "na"


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

        #years
        car['years'] = int(soup.select_one(".object_product_year").text.split('出廠')[0])
        #location
        car['location'] = "na"
        #doors
        car['doors'] = "na"
        #posttime
        strtime = soup.select(".number")[0].text
        timeArray = datetime.strptime(strtime, "%Y/%m/%d %H:%M")
        car['posttime'] = int(timeArray.timestamp())

        #content
        car['content'] = soup.select_one(".displayl-block.align-center").text.replace('\n', '').replace('\xa0','').replace('r','')
        #certificate
        car['certificate'] ="na"
        # soldout
        """soldout type is int ,there is no boolean type in sqlite3"""
        car['soldout'] = 0

        # price


        if (soup.select_one("p.price").text != "已收訂" or "面議"):
            car['price'] = round(int(soup.select_one("p.price").text.replace(",", "")) / 10000)

        else:
            car['soldout'] = 1
            car['price'] = 0

        #offtime
        car['offtime'] = ""


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
            car['type'] = "na"
            # gasoline $
            car['gasoline'] = infosoup[0].text.replace(" ", "")
            # transmission $
            car['transmission'] = infosoup[1].text.replace(" ", "")
            # color $
            car['color'] = infosoup[2].text
            # mileage $
            car['mileage'] = int(infosoup[3].text.replace("公里", "").replace(",", ""))
    except Exception as e:
        print("There is get_UCar_Item exception as below :{} URL: {}".format(e, url))
    return car

"""---------------------------------------------------------------"""

def add_to_sqlite(car,sqlite3_path):

    source = car['source']
    url   = car['url']
    title = car['title']
    brand = car['brand']
    model = car['model']
    doors = car['doors']
    color = car['color']
    gasoline = car['gasoline']
    cc = car['cc']
    transmission = car['transmission']
    equip = car['equip']
    mileage = car['mileage']
    years = car['years']
    location = car['location']
    posttime = car['posttime']
    price   = car['price']
    certificate = car['certificate']
    content = car['content']
    offtime = car['offtime']
    soldout = car['soldout']
    type = car['type']
    try:
        conn = sqlite3.connect(sqlite3_path)
        cursor = conn.cursor()
        cursor.execute('INSERT INTO ucar_price VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (source, url, title, brand, model, doors, color, gasoline, cc, transmission, equip, mileage, years, location, posttime, content, price , certificate , offtime, soldout ,type))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("There is add_to_sqlite exception as below :{} URL: {}".format(e, url))
    finally:
        conn.close()


"""---------------------------------------------------------------"""

#request these websites

def get_res(url):
    import requests
    headers = {
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
    get_res_count= 5
    while get_res_count > 0:
        try:
            res = requests.get(url, headers=headers, timeout=0.8)
            break
        except Exception as e:
            if get_res_count == 1:
                print("There is get_res exception as below :{} URL: {}".format(e, url))
            get_res_count -= 1
    return res

"""---------------------------------------------------------------"""
#This is main progress.

from bs4 import BeautifulSoup
import time
from datetime import datetime
import sqlite3
import random
import threading
car = {}
counts = 0

brand_list = ['toyota','benz','mazda','bmw','lexus','volkswagen','subaru','suzuki','nissan','audi','honda','volvo','porsche','ford','mitsubishi','mercedes-benz']
sqlite3_path = "C:\\Users\\BIG DATA\\Desktop\\test\\uCarPrice.sqlite3"
with open("C:\\Users\\BIG DATA\\Desktop\\test\\get_uCar_price.csv","r") as fr:
    for url in fr:
        url = url.strip()
        try:
            res = get_res(url)
            soup = BeautifulSoup(res.text, 'lxml')
            #record for crawling specific pages
            counts += 1
            if (counts % 50) == 0:
                print("Current link {}".format(counts))
            #the main function of getting second-hand car's information
            get_UCar_Item()

            #we get the car information which is year>=2008 and 15 brands
            if car['years'] < 2008:
                continue
            if not car['brand'] in brand_list:
                continue
            #write to sqlite3 database
            """the column type as below:
            source text, url text, title text, brand text, model text, color text, doors int, gasoline text, cc int, transmission text, equip text, mileage int, years int,location text, posttime int, type text, content text, certificate text,offtime text,soldout int
            """
            add_to_sqlite(car,sqlite3_path)
            time.sleep(random.choice(range(0, 1)))
            print(car)
        except Exception as e:
            print("There is main process exception as below :{} URL: {}".format(e, url))
            time.sleep(0.1)
