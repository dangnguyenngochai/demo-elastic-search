import connecting
import indexing 
import pandas as pd
import elasticsearch


def load_sample_data_csv(data_file='data/for-elastic-search.csv', index_col=0, usecols=None):
    if usecols:
        df = pd.read_csv(data_file, index_col=index_col, usecols=usecols).reset_index()
    else:
        df = pd.read_csv(data_file, index_col=index_col)
    return df

if __name__ == "__main__":
    print("loading data...")
    sample_cols = ['article_id', 'title', 'text']
    data_df = load_sample_data_csv(usecols=sample_cols)
    data_df = data_df[:10]

    print("connecting to eslastic instance...")
    client = connecting.connect2es()

    print("indexing data...")
    indexing.indexing_source('test-es-indexing', data_df, client)