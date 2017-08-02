def get_res(url):
    import requests.exceptions
    header = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
               "Accept-Encoding": "gzip, deflate, sdch, br",
               "Accept-Language": "zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4",
               "Cache-Control": "max-age=0",
               "Connection": "keep-alive",
               "Cookie": "__gads=ID=6dd06a6928bab430:T=1496776174:S=ALNI_MZw1RvLJ_ezD18iQcnLFUA0LsYbNQ; __RequestVerificationToken=oMBYkFk0gEocJ5npv8uUasjwq3v5YPCz5DB41DTuYdTP5P5feuleW8a7zBNYlUdZPxzPOBn4RRaktD4xSj1i-oMxRpg1;_gat=1; __asc=ec81b66215c834b6738ccfdfa0b; __auc=b3e83d1315c7ecf1e1aed870d7a; _ga=GA1.3.231933620.1496776122; _gid=GA1.3.1394749181.1496776122",
               "Upgrade-Insecure-Requests": "1",
               "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
               }
    count = 5
    while count > 0:
        try:
            res = requests.get(url, headers=header, timeout=5)
            break
        except Exception as e:
            if count == 1:
                print("There is get_res exception as below :{} URL: {}".format(e, url))
            count -= 1
    return res





def UCarReview_Crawler(writeFile,pageNo=10):

    dirlinks = []
    taglist  = ["#品牌與車型討論","#新品入手分享","#用車經驗分享","#購車經驗","#維修保養","#零配件及改裝"
                "#汽車科技","#產業觀察","#汽車其他綜合討論","#疑難發問","#賞車會","#間諜照","#車展"]


    for page in range(1,pageNo):
        try:
            res = requests.get(host+"?page=%s" % page)
            # print(page)
            soup = BeautifulSoup(res.text,'lxml')
            artlinks = soup.select("div.cell_topic_title > div.title > a")
            hashtag = soup.select_one("div.cell_topic_area > div.hashtag > a").text
            yearW = int(soup.select("div.user_group > p.text")[1].text.split('/')[0])
            yearR = int(soup.select("p.text")[4].text.split('/')[0])

            for artlink in artlinks:
                if hashtag in taglist:
                    links = artlink['href']
                    dirlinks.append(host+links)
            if(page%5==0):
                print("We are in %s" % page)
                time.sleep(1)
    #         if (yearW<2012 and yearR<2012):
    #             break

        except Exception as e:
            print("There is UCarReview_Crawler exception as below :{}".format(e))

    linklist = list(set(dirlinks))

    with open(writeFile,"w") as fw:
        fw.write('\n'.join(linklist)+ '\n')
        print("Total {} page".format(page))


import requests
from bs4 import BeautifulSoup
import time

host = "https://forum.u-car.com.tw/"
res  = get_res(host)
soup = BeautifulSoup(res.text,'lxml')

totalpage = 1+int(soup.select_one('a.arrow_right_2')['href'].split("page=")[1])
# print(totalpage)

UCarReview_Crawler("C:\\Users\\BIG DATA\\Desktop\\test\\get_uCar_review.csv")
#第一個參數需要修改連結位置
#第二個參數預設=10