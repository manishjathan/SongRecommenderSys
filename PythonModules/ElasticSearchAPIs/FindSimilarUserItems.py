import json
import time
import sys
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import csv
import pickle
from flask import Flask
from flask import jsonify

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

@app.route('/user_id/<string:uid>')
def getSimilarUsers(uid):
    es = connectToES();
    user_vector= es.get(index='user_vector_index',id=uid,doc_type='_doc',_source_includes = ["user_vector"])['_source']['user_vector']
    b = {"query" : {
                "script_score" : {
                    "query" : {
                            "match_all": {}
                        },
                    "script" : {
                            "source": "cosineSimilarity(params.query_vector, 'user_vector') + 1.0",
                            "params": {"query_vector": user_vector}
                        }
                    }
                 }
            }

    res = es.search(index='user_vector_index',body=b)
    sim_users_list = []
    for hit in res['hits']['hits']:
        sim_users_list.append(hit['_source']['user_id'])
    return jsonify(sim_users_list)
    
    
@app.route('/item_id/<string:iid>')
def getSimilarItems(iid):
    es = connectToES();
    item_vector= es.get(index='item_vector_index',id=iid,doc_type='_doc',_source_includes = ["item_vector"])['_source']['item_vector']
    b = {"query" : {
                "script_score" : {
                    "query" : {
                            "match_all": {}
                        },
                    "script" : {
                            "source": "cosineSimilarity(params.query_vector, 'item_vector') + 1.0",
                            "params": {"query_vector": item_vector}
                        }
                    }
                 }
            }

    res = es.search(index='item_vector_index',body=b)
    sim_items_list = []
    for hit in res['hits']['hits']:
        sim_items_list.append(hit['_source']['item_id'])
    return jsonify(sim_items_list)   
    
if __name__=="__main__":
    app.run(host='0.0.0.0', port=106)
    