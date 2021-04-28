import pickle
import SimilarityUtilityFunctions as sf
import UtilityFunctions as uf


##User:6706
if __name__=="__main__":
    simObj = sf.SimilarityObj()
    while(1):
        user_item_flag = input("Find Similarity for User or Item : ")
        if user_item_flag.lower() == 'item' or user_item_flag.lower() == 'user':
            id  = int(input("Enter Id : "))
            intersect = simObj.findIntersectingUserItems(user_item_flag.lower(),True,id,10,True)
            print(intersect)
        else:
            print("Flag didn't match")
            break
    
    