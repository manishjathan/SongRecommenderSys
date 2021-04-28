import numpy as np
from tqdm import tqdm


        
def fetchUserVectors(db,uid):
     user_songs = db.user_vectors.find({"user_id":uid},{"user_songs" : 1,"_id":0})
     song_ids = user_songs[0]['user_songs']
     
     user_vector_res = formQuery(db,song_ids,{'user_id':uid})
     user_vec = user_vector_res[0]['user_vector'].values()
     
     vector_res = formQuery(db,song_ids)
     other_user_vec = []
     for vector in vector_res:
         other_user_vec.append((vector['user_id'],list(vector['user_vector'].values())))
     return user_vec,other_user_vec

def formQuery(db,song_ids,find_clause={}):
    proj_cols = dict()
    for song_id in song_ids or []:
        proj_cols['user_vector.'+song_id] = 1
    proj_cols['_id'] = 0
    proj_cols['user_id'] = 1
    query_res = db.user_vectors.find(find_clause,proj_cols)
    return(query_res)


def FindSimilarUsers(db_obj,simObj,user_id):
    if db_obj.similarity_bucket.count_documents({'_id':user_id}) > 0:
        sim_items = db_obj.similarity_bucket.find({'_id':user_id},{'sim_items':1,'_id':0})
        return(sim_items[0]['sim_items'].values())
                
    else:
        print("Fetching User Vectors...")
        user_vec,other_user_vecs = fetchUserVectors(db_obj,user_id)
        min_pear = 0
        pear_sim_items = {}
        print("Computing Similarity...")
        for other_vec in tqdm(other_user_vecs):
            vec_1 = np.array(list(user_vec))
            vec_2 = np.array(list(other_vec[1]))
            usr_2 = other_vec[0]
                    
            msd_sim = simObj.msd(vec_1,vec_2)
            pear_sim = simObj.pearsonCorrelation(vec_1,vec_2)
                    
            if pear_sim > min_pear:
                if len(pear_sim_items) < 10:
                    pear_sim_items[str(pear_sim)] = usr_2 
                else:
                    min_sim_from_dict = simObj.return_min(pear_sim_items)
                    ## then if found similarity score is greater than minimum from dict
                    if pear_sim > min_sim_from_dict:
                        # then remove that item from dictionary
                        pear_sim_items.pop(str(min_sim_from_dict))
                                    
                        # update the min_sim
                        min_pear = min_sim_from_dict
                        pear_sim_items[str(pear_sim)] = usr_2
                        
        mongo_doc = {'sim_items':pear_sim_items}
        db_obj.similarity_bucket.update_one({'_id' : user_id},{'$setOnInsert' : mongo_doc},True)
        return(pear_sim_items.values())
            


            