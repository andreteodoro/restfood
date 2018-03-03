from elasticsearch import Elasticsearch
from retrying import retry
import requests

ES_HOST = {
    "host" : "elasticsearch.local", 
    "port" : 9200
}
RESTAURANTS_INDEX = 'restaurants'
DISHES_INDEX = 'dishes'

def retry_on_connectionerror(exc):
    return isinstance(exc, requests.exceptions.ConnectionError)

@retry(retry_on_exception=retry_on_connectionerror, wait_fixed=50000, stop_max_attempt_number=10)
def check_if_elasticsearch_is_up(es):
    res = requests.get('http://elasticsearch.local:9200')
    if res.status_code == 200:
        print('Elasticsearch is up and running!')

def delete_indexes(es):
    print("deleting '%s' index..." % (RESTAURANTS_INDEX))
    res = es.indices.delete(index = RESTAURANTS_INDEX)
    print(" response: '%s'" % (res))

    print("deleting '%s' index..." % (DISHES_INDEX))
    res = es.indices.delete(index = DISHES_INDEX)
    print(" response: '%s'" % (res))

def create_indexes(es):
    print("creating '%s' index..." % (RESTAURANTS_INDEX))
    res = es.indices.create(index = RESTAURANTS_INDEX)
    print(" response: '%s'" % (res))

    print("creating '%s' index..." % (DISHES_INDEX))
    res = es.indices.create(index = DISHES_INDEX)
    print(" response: '%s'" % (res))

def bulk_indexes(es):
    print("bulk indexing restaurants and dishes...")
    index_data = open('index-data','r')
    res = es.bulk(body = index_data.read())
    print(" response: '%s'" % (res))

def sanity_text(es):
    print("sanity test...")
    res = es.search(index = RESTAURANTS_INDEX, body = {"query": {"match_all": {}}})
    print(" response: '%s'" % (res))

def main():
    es = Elasticsearch(hosts = [ES_HOST])

    check_if_elasticsearch_is_up(es)
    print(es.info)
    if es.indices.exists(RESTAURANTS_INDEX) and es.indices.exists(DISHES_INDEX):
        delete_indexes(es)
    create_indexes(es)
    bulk_indexes(es)
    sanity_text(es)

if __name__ == "__main__": main()
