#open dict function、put the ten brands into word_dict
def open_dict(save_dict_path,brandName):
    with open(save_dict_path+brandName+".txt","r",encoding='utf-8') as fr:
        data=fr.read().lower().replace('-','_').strip().split('\n')
        dict_list = [word for word in data if len(word)>0]
        word_dict[brandName] = dict_list
    return word_dict

#open 10 brands dict
def get_brand_dict(save_dict_path):
    brandName= ['Benz','BMW','Ford','Honda','Lexus','Mazda','Mitsubishi','Nissan','Toyota','Volkswagen']
    for brand in brandName:
        open_dict(save_dict_path,brand)
        # print(word_dict)
    return word_dict

def get_brand_keyWord(column,word_dict):
    data[column] = data[column].lower().replace('-', '_')
    WdCuts = jieba.analyse.extract_tags(data[column])
    for WdCut in WdCuts:
        for key, word_list in word_dict.items():
            if WdCut in word_list:
                brand.append(key)
    print("There is title/content about {}".format(data[column]))
    return brand

from pymongo import MongoClient
import  jieba.analyse
word_dict = {}
save_dict_path = "E:\\PythonWin\\workspace\\dict\\carBrandDict\\branddict\\"
get_brand_dict(save_dict_path)
IP = 'localhost'
client = MongoClient(IP, 27017)
# Select database
db = client.test
# Select collection
collection = db.test
#get the article information
datas = collection.find()
#輪流開啟將在brandName的品牌檔案、並放入word_dict字典中
for idx, data in enumerate(datas):
    if idx < 10:
# for data in datas:
        brand = []
        #針對標題段詞、若有brand資訊、放入brand array中
        get_brand_keyWord("title",word_dict)
        #針對文章的內容段詞、若有brand資訊、放入brand array中
        get_brand_keyWord("content",word_dict)
        brand = list(set(brand))
        print("This brand tag is {}".format(brand))
        # db.test.find_one_and_update({'_id':data['_id']},{"$set":{"brand":brand}})
print("all brand-tag of articles finished!")