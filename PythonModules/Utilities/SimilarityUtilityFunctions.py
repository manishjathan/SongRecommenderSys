import pandas as pd
import numpy as np
import pickle
import UtilityFunctions as uf
from tqdm import tqdm

class SimilarityObj:
    
    def __init__(self):
        self.train_set_obj = uf.returnObj('D:\\Million Song Dataset\\train_set_obj')
        self.train_data = uf.returnObj('D:\\Million Song Dataset\\train_data')
        self.svd = uf.returnObj('D:\\Million Song Dataset\\svd')
        self.train_set_test_obj = uf.returnObj('D:\\Million Song Dataset\\train_set_test_obj')
        
    def predictRatings(self):
        return(self.svd.test(self.train_set_test_obj))
        
    def findSimilarUsers(self,u1):
        ## Get the list of all Users
        unique_users = self.train_data['user_id'].unique()
        # Iterating through all the users
        for u2 in tqdm(unique_users):
            if u1!=u2:
                common_songs = self.findCommonSongs(u1,u2)
                if common_songs != 0:
                    print("UserId : " ,u2)
                    print("Common Songs : ",common_songs)
    
    def findCommonSongs(self,u1,u2):
        u1_songs = set(self.train_data[train_data['user_id']==u1]['song_id'].values)
        u2_songs = set(self.train_data[train_data['user_id']==u2]['song_id'].values)
        common_songs = u1_songs.intersection(u2_songs)
        if len(common_songs)==0:
            return 0
        else:
            return common_songs
            
    def cosineSimilarity(self,vec_1,vec_2):
        std_i1_vec = np.sqrt(np.sum(np.square(vec_1)))
        std_i2_vec = np.sqrt(np.sum(np.square(vec_2)))
        sim = (np.dot(vec_1,vec_2))/((std_i1_vec*std_i2_vec))
        return(round(sim,2))
        
    def msd(self,vec_1,vec_2):
        sim = (np.sum(np.square(vec_1-vec_2)))/len(vec_1)
        return(round(sim,2))
    
    def pearsonCorrelation(self,vec_1,vec_2):
        i1_cval = vec_1-np.mean(vec_1)
        i2_cval = vec_2-np.mean(vec_2)
        i1_std_dev = np.sqrt(np.sum(np.square(i1_cval)))
        i2_std_dev = np.sqrt(np.sum(np.square(i2_cval)))
        num = np.sum(i1_cval*i2_cval)
        denom = i1_std_dev * i2_std_dev
        sim = num/denom
        return(round(sim,2))

    def return_min(self,similar_items_dict):
        min = 10000
        for value in similar_items_dict.keys():
            val = float(value)
            if val < min:
                min = val
        return min
        
    def computeUserItemVector(self,ui,user_item_flag='user'):
        ## Prepare the list of item vector
        unique_songs = self.train_data.song_id.unique()
        triplet_list = []
        mean_rating = np.mean(self.train_data['play_count'])
        ## Create user,item,rating triplet
        for song in unique_songs:
            triplet_list.append((ui,song,mean_rating))
        pred = self.svd.test(triplet_list)
        est_values = pd.DataFrame(pred)['est'].values
        return unique_songs,est_values
        
    def computeUserItemSimilarity(self,user_item_flag,latent_factor_flag,i1,i2,sim_method):
        ##Converting raw_ids to svd's internal representation
        sim = 0
        vec_1 = np.array(20)
        vec_2 = np.array(20)
        
        if user_item_flag=='user' and latent_factor_flag==True:
            id_1 = self.train_set_obj.to_inner_uid(i1)
            id_2 = self.train_set_obj.to_inner_uid(i2)
            vec_1 = self.svd.pu[id_1]
            vec_2 = self.svd.pu[id_2]
        
        elif user_item_flag == 'item' and latent_factor_flag==True:
            id_1 = self.train_set_obj.to_inner_iid(i1)
            id_2 = self.train_set_obj.to_inner_iid(i2)
            vec_1 = self.svd.qi[id_1]
            vec_2 = self.svd.qi[id_2]
        
        
        # Cosine similarity between items
        if sim_method == 'cosine':
            sim = self.cosineSimilarity(vec_1,vec_2)
        
        ## Mean Squared Distance
        elif sim_method == 'msd':
            sim = self.msd(vec_1,vec_2)
            
        ## Pearson Correlation Coefficient
        elif sim_method == 'pearson':
            sim = self.pearson(vec_1,vec_2)
            
        return sim
    def findSimilarUserItems(self,user_item_flag,latent_factor_flag,i1,similarity,k):
        
        unique_user_items = []
        if user_item_flag == 'user':
            unique_user_items = self.train_data['user_id'].unique()
        elif user_item_flag == 'item':
            unique_user_items = self.train_data['song_id'].unique()
            
        #Declaring a dictionary to maintain similar items for a given item
        top_k_similar_user_items = {}
        min_sim = 0
        
        #Iterating through all items/users
        for i2 in tqdm(unique_user_items):
            if i1!=i2:
                sim = abs(self.computeUserItemSimilarity(user_item_flag,latent_factor_flag,i1,i2,similarity))
                
                ## if similarity is greater than min_sim 
                if sim > min_sim:
                    ## then simply append till we find top 10 users
                    if len(top_k_similar_user_items) < k:
                        top_k_similar_user_items[sim] = i2
                    else:
                        ## after finding the top k
                        ## we first find the minimum from the dictionary
                        min_sim_from_dict = self.return_min(top_k_similar_user_items)
                        
                        ## then if found similarity score is greater than minimum from dict
                        if sim > min_sim_from_dict:
                            # then remove that item from dictionary
                            top_k_similar_user_items.pop(min_sim_from_dict)
                            
                            # update the min_sim
                            min_sim = min_sim_from_dict
                            top_k_similar_user_items[sim] = i2
                            
        return top_k_similar_user_items
        
    def findIntersectingUserItems(self,user_item_flag,latent_factor_flag,i1,k,verbose=False):
        cosine_sim_items = set(self.findSimilarUserItems(user_item_flag,latent_factor_flag,i1,'cosine',k).values())
        msd_sim_items = set(self.findSimilarUserItems(user_item_flag,latent_factor_flag,i1,'msd',k).values())
        pearson_sim_items = set(self.findSimilarUserItems(user_item_flag,latent_factor_flag,i1,'pearson',k).values())
        intersect = cosine_sim_items.intersection(pearson_sim_items)
        
        if verbose == True:
            print("Cosine similarity : ")
            print(cosine_sim_items)
            print("Msd similarity : ")
            print(msd_sim_items)
            print("Pearson similarity : ")
            print(pearson_sim_items)
        
        return intersect