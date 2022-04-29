from elasticsearch import Elasticsearch
from pandas import DataFrame
from elasticsearch import Elasticsearch
import numpy as np

def predict_all_revelants_doc(es: Elasticsearch, 
                        index: str, 
                        question_df: DataFrame, 
                        query_on: str,
                        top_k=10):
    predicts = []
    for q in question_df.values:
        resp = query4phrase(es, index, q, query_on, return_field=['_id'], top_k=10)

        hits = resp.get('hits').get('hits')
        _ids = [hit.get("_id") for hit in hits]
        predicts.append(_ids)
    return np.array(predicts)
    
def query4phrase(es: Elasticsearch, 
                index: str, 
                question: str, 
                query_on: str, 
                return_field=['_source'],
                top_k=10):               
    body={}
    query_formulation = {
        "match": {
            query_on:{
                "query": question,
            }
        },
    }
    body.update({'query': query_formulation})
    resp = es.search(index=index, query=query_formulation, size=top_k, stored_fields=return_field)
    # print("hello")
    return dict(resp)

if __name__=='__main__':
    pass
else:
    print("imported query handler")