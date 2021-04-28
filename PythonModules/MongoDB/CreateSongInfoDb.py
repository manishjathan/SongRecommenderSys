import pandas as pd
import numpy as np
import sys
sys.path.insert(1, 'D:\\Million Song Dataset\\PythonModules\\Utilities')
from pymongo import MongoClient
import UtilityFunctions as uf
from tqdm import tqdm

def connectToDb(mongoUrl):
    try:
        client = MongoClient(mongoUrl)
        db=client.testdb
        print("*"*100)
        print("Connected to testdb")
        return db
    except:
        print("Unable to connect to testdb")


if __name__ == "__main__":
    
    db_obj = connectToDb("mongodb://localhost:27017")
    song_info_db = uf.returnObj("D:\\Million Song Dataset\\song_info_db")
    song_info_db.reset_index(inplace=True)
    data_dict = song_info_db.to_dict("records")
    db_obj.song_info_db.insert_many(data_dict)
        
