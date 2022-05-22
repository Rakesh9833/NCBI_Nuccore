from elasticsearch import Elasticsearch

es_hosts = [
    "http://localhost:9200",
]

es_api_user = 'elastic'
es_api_password = '7JeUVlLMTmi8U00VKqSZ'


def elastic_upload():
    es = Elasticsearch(
        es_hosts,
        http_auth=(es_api_user, es_api_password),
    )
    return es
