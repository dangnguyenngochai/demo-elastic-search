from elasticsearch import Elasticsearch
from dotenv import dotenv_values

config = dotenv_values(".env")

def connect2es():
    # print(config)
    client = Elasticsearch(config.get("HOST"), 
                            verify_certs=False,                # disable verify TLS/SSL 
                            basic_auth=(config.get("USERNAME"),config.get("PASSWORD")))   # provide the credentials
    return client

if __name__ == "__main__":
    pass
else:
    print("imported connecting library")
