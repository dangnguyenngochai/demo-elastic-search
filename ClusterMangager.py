from elasticsearch import Elasticsearch
import pandas as pd

def get_all_index(client: Elasticsearch):
    indices = [key for key in dict(client.indices.get(index="*")).keys()]
    return indices

def delete_source(index_name: str, client: Elasticsearch):
    resp = client.indices.delete(index=[index_name])
    resp = dict(resp)
    if resp.get("acknowledged"):
        print("index ", index_name, "deleted")
    else:
        print("Something wrong happen! Check if the source is already deleted")

def indexing_source(index_name: str,
                    data_frame: pd.DataFrame or pd.Series,
                    client:Elasticsearch,
                    _id_: str):

    def index_doc(doc: dict, idx: int):
        try:
            _id = doc.pop("_id")
        except KeyError:
            print('Error at document', idx, ': Need to have keys "_id"')
            _id = None
        else:
            try:
                # print(doc)
                client.index(index=index_name, id=_id, document=doc)
            except Exception:
                print("Error at", _id, type(_id))

    df_temp = data_frame.copy()
    df_temp = df_temp.rename({str(_id_): '_id'}, axis=1)
    for idx in range(len(df_temp)):
        row = df_temp.iloc[idx].to_dict()
        # print(row)
        if idx%100==0:
            print("indexing at", idx)
        index_doc(row, idx)

if __name__ == "__main__":
    pass
else:
    print("imported indexing library")