import elasticsearch as es
import pandas as pd


def indexing_source(index_name: str,
                    data_frame: pd.DataFrame,
                    client: es.Elasticsearch):

    def index_doc(doc: dict):
        try:
            _id = doc.pop("_id")
        except KeyError:
            _id = None
        finally:
            client.index(index=index_name, id=_id, document=doc)

    for idx in range(len(data_frame)):
        row = data_frame.iloc[idx].to_dict()
        if idx%100:
            print("indexing at", idx)
        index_doc(row)

if __name__ == "__main__":
    pass
else:
    print("imported indexing library")