import sys
sys.path.insert(1, 'D:\\Million Song Dataset\\PythonModules\\Utilities')
import SimilarityUtilityFunctions as sf
from pymongo import MongoClient
from tqdm import tqdm
import pandas as pd
import pickle

# pprint library is used to make the output look more pretty

#mongodb://18.225.33.219:27017

def connectToDb(mongoUrl):
    try:
        client = MongoClient(mongoUrl)
        db=client.testdb
        print("Connected to testdb")
        return db
    except:
        print("Unable to connect to testdb")

def insertDataIntoDb(db):
    simObj = sf.SimilarityObj()
    users = simObj.train_data.user_id.unique()
    for user in tqdm(users) :
        items,pred_res = simObj.computeUserItemVector(user)
        items = [str(item) for item in items]
        
        ## Predicting ratings
        pred_res = [round(res,2) for res in pred_res]
        pred_ratings = dict(zip(items,pred_res))
        
        ## Creating list of user songs
        user_songs = list(simObj.train_data[simObj.train_data.user_id==int(user)]['song_id'].unique())
        user_songs = [str(song) for song in user_songs]
        
        ## Creating Object
        obj_to_insert = {"user_id":str(user),"user_songs":user_songs,"user_vector":pred_ratings}
        
        res = db.user_vectors.insert_one(obj_to_insert)
        
    
    
if __name__=="__main__":
    db_obj = connectToDb("mongodb://localhost:27017")
    insertDataIntoDb(db_obj)
    