# #open dict function、put the ten brands into word_dict
def open_dict(path,feature):
    with open(path+feature+".txt","r",encoding='utf-8') as fr:
        data=fr.read().lower().replace('-','_').strip().split('\n')
        dict_list = [word for word in data if len(word)>0]
        word_dict[feature] = dict_list
    return word_dict

#open featrue dict
def get_featured_dict(path):
    feature_list= ['comfortable','fuelEco','safe','space','beauty','preservation']
    for feature in feature_list:
        open_dict(path,feature)
#         print(word_dict)
    return word_dict


from pymongo import MongoClient
import jieba

word_dict = {}

path = "E:\\PythonWin\\workspace\\dict\\featuredDict\\"  #**四本特徵詞路徑修改***
# 輪流開啟將在特徵詞字典、並放入word_dict字典中
get_featured_dict(path)
IP = '10.120.37.11'
client = MongoClient(IP, 27017)
#Select database
db = client.final_project
# Select collection
collection = db.mobile01
# get the article information
datas = collection.find()
feature = []
reply = []
comfortable_count = 0
fuelEco_count = 0
safe_count = 0
space_count = 0
beauty_count = 0
preservation_count = 0
article_concat =""
#**load車子字典 路徑修改***
jieba.load_userdict("C:\\Users\\BIG DATA\\Desktop\\dict&others\\yahoo_content(0702).txt")
jieba.load_userdict("E:\\PythonWin\\workspace\\dict\\featuredDict\\allFreq.txt")
jieba.load_userdict("E:\\PythonWin\\workspace\\dict\\moedDict\\moedict.txt")

for idx, data in enumerate(datas):
    if idx<20:
        if idx%10==0:
            print("目前處理到第{}篇文章".format(idx))
        # 每篇文章的主要內容和回覆內容連接成一段話
        article_concat +=data['content']
        for replies in data['replies']:
            article_concat += replies['content']

        strCuts=jieba.cut(article_concat,cut_all=False)
        strWd=",".join(strCuts)
        articleList = strWd.split(',')
        # print(articleList)

        # 針對文章的內容段詞、若有特徵詞資訊、放入feature array中
        for content in articleList:
            for key, word_list in word_dict.items():
                if content in word_list:
                    feature.append(key)
        feature = list(set(feature))
        #計算出現在feature的標籤次數
        for countFeature in feature:
            if "comfortable" == countFeature:
                comfortable_count += 1
            elif "fuelEco" == countFeature:
                fuelEco_count +=1
            elif "safe" == countFeature:
                safe_count +=1
            elif "space" ==countFeature:
                space_count +=1
            elif "preservation" == countFeature:
                preservation_count +=1
            elif "beauty" ==countFeature:
                beauty_count +=1
            else:None
        print("原本的文章內容:{}".format(article_concat))
        print("舒適={},省油={},安全={},空間={},保值={},美觀={}"
              .format(comfortable_count, fuelEco_count, safe_count, space_count, preservation_count, beauty_count))
        print(feature)
        print("============================================================================")
        #文章內容、feature Tag array、次數歸0
        article_concat = ""
        feature=[]
print("count finished!")
        # comfortable_count = 0
        # fuelEco_count = 0
        # safe_count = 0
        # space_count = 0

