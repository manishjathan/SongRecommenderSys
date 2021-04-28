import sys
sys.path.insert(1, 'D:\\Million Song Dataset\\PythonModules\\Utilities')
import SimilarityUtilityFunctions as sf
from pymongo import MongoClient
import FindSimilarUserRecords as sr


## FetchRecords from DB
def connectToDb(mongoUrl):
    try:
        client = MongoClient(mongoUrl)
        db=client.testdb
        print("*"*100)
        print("Connected to testdb")
        return db
    except:
        print("Unable to connect to testdb")
  
def findSimilarSongs(db,simObj,sim_users):
    user_songs = db.user_vectors.find({'user_id': {'$in' : list(sim_users)}},{'_id':0,'user_songs':1})
    superset = set([])
    for user_song in user_songs:
        if len(superset) == 0:
            superset = set(user_song['user_songs'])
        else:
            superset = superset.union(set(user_song['user_songs'])) 
    
    superset = list(superset)
    superset = [int(i) for i in superset]
    
    song_info = db.song_info_db.find({'song_id' : { '$in' : superset}},{'_id':0,'artist_name':1,'song_name':1,'song_id':1})
    for song in song_info:
        print(song)
        
if __name__=='__main__':
    db = connectToDb("mongodb://localhost:27017")
    simObj = sf.SimilarityObj()
    while(1):
        user_id = input("Enter User Id : ")
        if user_id == 'end':
            break
        else:
            similar_users = sr.FindSimilarUsers(db,simObj,user_id)
            findSimilarSongs(db,simObj,similar_users)
        