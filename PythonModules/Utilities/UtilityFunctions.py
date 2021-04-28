## fetchRecords from DB
import pandas as pd
import numpy as np
import pickle
from tqdm import tqdm
from os import path


def getQuantileParam(m):
    """
        m : %iles in which it has to be divided
        if m = 10 then quantiles = [0,10,20,30...100]
    """
    return [i/100 for i in range(0,101,m)]
    
    
def returnObj(fileName):
    """
        fileName : filePath to pick the object
    """
    if path.exists(fileName):
        sampleFile = open(fileName,'rb')
        obj = pickle.load(sampleFile)
        sampleFile.close()
        return obj
    else:
        return "fileName doesn't exist"
        
def dumpObj(obj,fileName):
    """
        obj :  object to dump
        filename : filepath where the object has to be dumped
    """
    sampleFile = open(fileName,'wb')
    pickle.dump(obj,sampleFile)
    sampleFile.close()