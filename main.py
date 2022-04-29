import Connector
import ClusterMangager 
import pandas as pd
import numpy as np

def gen_report(preds, labels):
    score = 0
    size = len(labels)
    mask = preds == np.expand_dims(labels, axis=1)
    preds_rankings = 1/(np.argmax(mask, axis=1) + 1)
    for preds, labels in zip(preds, labels):
        if labels in preds:
            score +=1 
    line_format = '{:<20} || {:>20}'
    print("="*40)
    print(line_format.format('Precision', float(score)/size))
    print(line_format.format("Mean Reciporal Rank", np.mean(preds_rankings)))
    print("="*40)

def load_data(data_file, index_col=0, usecols=None):
    try:
        with open(data_file) as _:
            if usecols:
                df = pd.read_csv(data_file, index_col=index_col, usecols=usecols, keep_default_na=False).reset_index()
            else:
                df = pd.read_csv(data_file, index_col=index_col,  eep_default_na=False)
            return df
    except Exception as ex:
        print("Something went wrong !Detail is shown below:")
        print("="*20)
        print(ex)
        print("="*20)

def hardcode_init():
    data_file = 'data/for-elastic-search.csv'
    query_file = 'data/legal_query.csv'
    print("loading data...")
    sample_cols = ['full', 'doc_id']
    query_cols = ['question', 'doc_id']
    print("done loading")
    data_df = load_data(data_file = data_file, usecols=sample_cols)
    query_df = load_data(data_file = query_file, usecols=query_cols)
    data_df = data_df[:10]
    print("connecting to eslastic instance...")

    client = Connector.connect2es()
    print("connected to elastic client")
    print("indexing data...")
    ClusterMangager.indexing_source('test-es-indexing', data_df, client, _id_='article_id')
    print("done indexing")

if __name__ == "__main__":
    hardcode_init()
else:
    print("imported elastic pipeline")