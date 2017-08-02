# get the aritcle attribution
def get_UCar_Article(url):
    try:
        # url
        review['url'] = url
        for history in soup.select('.cell_post_area.history_story'):
            history.decompose()
        for img in soup.find_all("img"):
            img.decompose()
        # title
        review['title'] = soup.select_one(".cell_post_title").text
        # forum class
        review['hashtag'] = soup.select(".hashtag > a ")[0].text.replace('\n', '').replace('#', '')
        # source
        review['source'] = "UCar"
        # brand tag
        if len(soup.select('hashtag > a')) >= 2:
            review['brand'] = soup.select(".hashtag > a ")[1].text.replace('#', '')
        else:
            review['brand'] = ""

    except Exception as e:
        print("There is get_UCar_Article exception as below :{} URL: {}".format(e, url))
    time.sleep(0.1)
    return review


"""------------------------------------------------------------------------------------"""
# address detail of article content

"""get rid of useless tag (br,tr,quote,img,history) and get the auther ,content,replyer ,reply content"""


def get_UCar_Replies_item(i,replyNo):
    try:
        review['reply_no'] = replyNo
        # get the total reply counts(contain author) of "this page"
        replyCount = len(soup.select(".order"))
        # the auther's floor ,so replyCount +1
        if i == 0:
            replyCount += 1

        for j in range(replyCount):
            # auther ID ,reply ID
            replyer = soup.select('.writer.toggle_profile > a')[j].text
            # PO time,reply time
            strtime = soup.select('.user_group > p.text')[(2 * j + 1)].text
            timeArray = datetime.strptime(strtime, "%Y/%m/%d %H:%M:%S")

            # original content
            dircontent = soup.select('.cell_post_area > div.comment')[j]

            # get rid of useless tag
            for tr in dircontent.find_all('tr'):
                tr.extract()
            for br in dircontent.find_all('br'):
                br.decompose()
            for history in soup.select('.cell_post_area.history_story'):
                history.decompose()
            for quote in soup.select('.comment_quote'):
                quote.decompose()
            for img in soup.find_all("img"):
                img.decompose()

            # clean content
            replycontent = dircontent.text.strip()

            # get the author's ID & content where is 1th page and 1th floor
            if i == 0 and j == 0:
                review['author'] = replyer
                review['tm'] = int(timeArray.timestamp())
                review['content'] = replycontent
            # get the replyer's ID & content

            elif j==1 and replyNo ==0:
                reply_dict = {}
                reply_dict['tm'] = 0
                reply_dict['content'] = ""
                reply_dict['author_id'] = ""
                replies.append(reply_dict)
            else:
                reply_dict = {}
                reply_dict['tm'] = int(timeArray.timestamp())
                reply_dict['content'] = replycontent
                reply_dict['author_id'] = replyer
                replies.append(reply_dict)

            review['replies'] = replies
    except Exception as e:
        print("There is get_UCar_Replies_item exception as below :{} URL: {}".format(e, url))
    return review


"""------------------------------------------------------------------------------------"""
# get article content.

"""there are two if statement

    (1) there are replies(include author) and total reply page =1
    (2) there are replies(include author) and total reply page >1
"""


def get_UCar_Replies(url):
    global soup
    global replies
    global res
    try:
        replyNo = int(soup.select_one('.comment_top > div.title > p.number').text)
        replyPage = int(soup.select_one(".text-black-14").text)
        replies = []

        if replyPage == 1:
            for i in range(replyPage):
                get_UCar_Replies_item(i,replyNo)


        # elif replyNo != 0 and replyPage == 1:
        #     for i in range(replyPage):
        #         get_UCar_Replies_item(i,replyNo)

        elif replyNo != 0 and replyPage > 1:
            for i in range(replyPage):

                if i != 0:
                    nextpage = i + 1 if i < replyPage else i
                    nextlink = url + "?page={}#replies".format(nextpage)
                    res = get_res(nextlink)
                    soup = BeautifulSoup(res.text, 'lxml')
                get_UCar_Replies_item(i,replyNo)

        time.sleep(0.5)

    except Exception as e:
        print("There is get_UCar_Replies exception as below :{} URL: {}".format(e, url))
    return review,replyNo


"""------------------------------------------------------------------------------------"""


# add data into database

def add_to_sqlite(review,sqlite3_path):
    source = review['source']
    url = review['url']
    title = review['title']
    author = review['author']
    tm = review['tm']
    hashtag = review['hashtag']
    content = review['content']
    replies = json.dumps(review['replies'])
    brand = review['brand']
    reply_no = review['reply_no']
    try:
        conn = sqlite3.connect(sqlite3_path)
        cur = conn.cursor()
        cur.execute('INSERT INTO ucar_review VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (source, url, title, author, tm, hashtag, content, replies, brand , reply_no))

        """the colum type as below:
        source text, url text, title text, author text, tm int, hashtag text, content text, replies text, brand text ,reply_no int
        """
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("There is trouble in writing to database {} {} ".format(e, url))
    finally:
        conn.close()
    """------------------------------------------------------------------------------------"""


# request these websites

def get_res(url):
    import requests
    header = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
              "Accept-Encoding": "gzip, deflate, sdch, br",
              "Accept-Language": "zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4",

              "Cache-Control": "max-age=0",
              "Connection": "keep-alive",
              "Cookie": "__gads=ID=6dd06a6928bab430:T=1496776174:S=ALNI_MZw1RvLJ_ezD18iQcnLFUA0LsYbNQ; __RequestVerificationToken=oMBYkFk0gEocJ5npv8uUasjwq3v5YPCz5DB41DTuYdTP5P5feuleW8a7zBNYlUdZPxzPOBn4RRaktD4xSj1i-oMxRpg1;_gat=1; __asc=ec81b66215c834b6738ccfdfa0b; __auc=b3e83d1315c7ecf1e1aed870d7a; _ga=GA1.3.231933620.1496776122; _gid=GA1.3.1394749181.1496776122",
              "Upgrade-Insecure-Requests": "1",
              "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}
    count = 5
    while count > 0:
        try:
            res = requests.get(url, headers=header, timeout=2)
            # print("get_res:",res)
            count = 0
            break
        except Exception as e:
            if count == 1:
                print("There is get_res exception as below :{} URL: {}".format(e, url))
            count -= 1
    return res


"""------------------------------------------------------------------------------------"""


# Thread of getting article

def get_UCarReview(url):
    global soup
    global res
    try:
        sqlite3_path = "C:\\Users\\BIG DATA\\Desktop\\test\\uCarReview.sqlite3"
        res = get_res(url)
        soup = BeautifulSoup(res.text, 'lxml')
        get_UCar_Article(url)
        get_UCar_Replies(url)
        add_to_sqlite(review,sqlite3_path)
        print(review)
    except Exception as e:
        print("There is get_UCarReview_th exception as below :{} URL: {}".format(e, url))
    return url


"""------------------------------------------------------------------------------------"""
# This is main progress.

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import random
import sqlite3
import threading
import json
import multiprocessing

review = {}
link_list = []

if __name__ == "__main__":
    with open("C:\\Users\\BIG DATA\\Desktop\\test\\get_uCar_review.csv", "r") as fr:
        links = fr.read().split('\n')
        # print(links)
        for link in links:
            link_list.append(link)
            if len(link_list) == 3:
                pool = multiprocessing.Pool(processes=8)
                res = pool.map(get_UCarReview,link_list)
                pool.close()
                link_list = []
