import indexing
import pandas
from elasticsearch import Elasticsearch
es = Elasticsearch()

def load_sample_data_csv(data_file='for-elastic-search'):
    df = pd.read_csv(data_file)
    return df

def indexing_source(index_name, data_frame, field=[]):
    for idx, data in enumerate(data_frame):
        field = field if len(field) else list(data_frame.columns)
        
        data_to_index = data.iloc[:,field]
       
        es.index(index="index_name", id=idx, document=data)
    
    return resp

if __name__ == "__main__":
    print("in main")
    print("indexing source")
    df = load_sample_data_csv()
    indexing_source('test', df)