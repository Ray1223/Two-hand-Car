#request these websites

def get_res(url):
    import requests.exceptions
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
    count = 5
    while count > 0:
        try:
            res = requests.get(url, headers=headers, timeout=2)
            break
        except Exception as e:
            if count == 1:
                print("There is get_res exception as below :{} URL: {}".format(e, url))
            count -= 1
    return res

"""---------------------------------------------------------------"""


def UCarPrice_Crawler(writeFile, pageNo=10):
    dirLinks = []
    brandList = ['toyota', 'mercedes-benz' , 'mazda', 'bmw', 'lexus', 'volkswagen', 'subaru', 'suzuki', 'nissan', 'audi',
               'honda', 'volvo', 'porsche', 'ford', 'mitsubishi']
    for page in range(1, pageNo):
        try:
            #record for crawling specific pages
            if (page % 5 == 0):
                print("We are in {}th page".format(page))
                time.sleep(1)
            #get every link in every page
            carLink = host + "index.aspx?page={}&during=1".format(page)
            res = get_res(carLink)
            soup = BeautifulSoup(res.text, 'lxml')
            partLinks = soup.select(".title > .clean_a_tyle")
            for partLink in partLinks:
                brand = partLink.text.split()[0].lower()
                if brand in brandList:
                    links = partLink['href']
                    dirLinks.append(host + links)

        except Exception as e:
            print("There is UCarPrice_Crawler exception as below :{} URL: {}".format(e, url))
            break
    #comfirm to get unrepeatable link
    linkList = list(set(dirLinks))

    with open(writeFile, "w") as fw:
        fw.write('\n'.join(linkList) + '\n')
        print("download page %s" % page)
    return print("download URL finished!"), dirLinks




"""---------------------------------------------------------------"""
#This is main progress.

from bs4 import BeautifulSoup
import time

host = "https://usedcar.u-car.com.tw/"

try:
    #get the totalpage of U-Car second-hand Car's website
    res  = get_res(host)
    soup = BeautifulSoup(res.text, 'lxml')
    totalpage = int(soup.select_one('li.arrow_right_2 > a')['href'].split("page=")[1].split("&during=1")[0])
    # print(totalpage)

    #the main function of getting link
    """first parameter: the file location of output
       second parameter :how many pages do we want to crawl,default =10"""
    UCarPrice_Crawler("C:\\Users\\BIG DATA\\Desktop\\test\\get_uCar_price.csv")
    # 第一個參數需要修改連結位置
    # 第二個參數預設=10
except Exception as e:
    print("There is main process exception as below :{} URL: {}".format(e, url))



