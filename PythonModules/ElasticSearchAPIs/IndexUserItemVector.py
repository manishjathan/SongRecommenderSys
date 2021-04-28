import json
import time
import sys
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import csv
import pickle
from flask import Flask
from flask import request
import _thread

app = Flask(__name__)

def connectToES():
    es = Elasticsearch([{'host': '52.15.188.222', 'port': 9200}])
    if es.ping():
        print('Connected to ES!')
    else:
        print('Could not connect!')
        sys.exit()
    print("*********************************************************************************");
    return(es)
    
    
def returnObj(obj):
    sampleFile = open(obj,'rb')
    ret_obj = pickle.load(sampleFile)
    sampleFile.close()
    return(ret_obj)

@app.route('/index',methods=["POST"])
def createIndex():
    ## Let's create our index for storing the data
    print("*"*100)
    ## Taking all the required inputs from the API
    user_item_flag = request.args.get("user_item_flag")
    index_name = request.args.get("index_name")
    index_body = request.json
    print("User_Item_Flag : ",user_item_flag)
    print("Index_name : ",index_name)
    print("Index Schema : ",index_body)
    
    es = connectToES()
    ret = es.indices.create(index=index_name, ignore=400, body=index_body) #400 caused by IndexAlreadyExistsException, 
    print(json.dumps(ret,indent=4))
    print("*"*100)
    try:
        _thread.start_new_thread(indexData,(es,user_item_flag,index_name,))
    except:
        print("ERROR:Unable to start thread")
        
    return ("Indexing Data in",index_name,"...")
    
 
def indexData(es,user_item_flag,index_name): 
    train_data = returnObj("D:\\Million Song Dataset\\train_data")
    train_set_obj = returnObj("D:\\Million Song Dataset\\train_set_obj")
    svd_obj = returnObj("D:\\Million Song Dataset\\svd")
    if user_item_flag == 'user':
        user_ids = train_data.user_id.unique()
        del(train_data)
        count = 0
        for user_id in user_ids: 
            uid = train_set_obj.to_inner_uid(user_id)
            index_doc ={
                "user_id":user_id,
                "user_vector":svd_obj.pu[uid]
            }
            res = es.index(index = index_name,id=user_id,body=index_doc)
            if count%1000 == 0:
                print("Processed 1000 records")
            count = count+1
        
    elif user_item_flag == 'item':
        item_ids = train_data.song_id.unique()
        del(train_data)
        count = 0
        for item_id in item_ids: 
            iid = train_set_obj.to_inner_iid(item_id)
            index_doc ={
                "item_id":item_id,
                "item_vector":svd_obj.qi[iid]
            }
            res = es.index(index = index_name,id=item_id,body=index_doc)
            if count%1000 == 0:
                print("Processed 1000 records")
            count = count+1
    else:
        print("Invalid flag")
        
        
if __name__=="__main__":
    app.run(host='0.0.0.0', port=105)
    