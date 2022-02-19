import urllib.request
from urllib.error import HTTPError
import json

BASE_URL = "http://localhost:9200"
BASE_INDEX = "covid_ccse-"

# Doc counts are around 3100 per day with about 650KB so 1 shard is good
SHARDS = 1
# Loading data locally into a 1 node cluster so no replicas, if I lose data I reload it. Backed up in Github.com
REPLICAS = 0

def es_http(method: str, url: str, record: dict = {}) -> None:
    header = {"Content-Type": "application/json"}
    if record:
        request = urllib.request.Request(url=url, data=json.dumps(record).encode('utf8'), method=method,
                                         headers=header)
    else:
        request = urllib.request.Request(url=url, method=method, headers=header)
    urllib.request.urlopen(request)


def es_create_index(index: str):
    index_url = f"{BASE_URL}/{index}"
    print(f"DELETE {index_url}")
    try:
        es_http('DELETE', index_url)
    except HTTPError as ex:
        if ex.code != 404:
            raise Exception(f"Could not delete index {index}: HTTP {ex.code}")
    except Exception as ex:
        raise Exception(f"Could not delete index {index}: {ex}")


    settings = {
        "settings": {
            "number_of_shards": SHARDS,
            "number_of_replicas": REPLICAS,
        },
        "mappings": {
            "_source": {"enabled": True},
            "dynamic": False,
            "properties": {
                "fips": {"type": "keyword"},
                "county": {"type": "keyword"},
                "state": {"type": "keyword"},
                "country": {"type": "keyword"},
                "last_update": {"type": "date"},
                "location_point": {"type": "geo_point"},
                "confirmed": {"type": "long"},
                "deaths": {"type": "long"},
                "recovered": {"type": "long"},
                "active": {"type": "long"},
                "location_desc": {"type": "text"},
            }
        }
    }
    try:
        es_http('PUT', index_url, settings)
    except HTTPError as ex:
        raise Exception(f"Could not write mapping for {index}: HTTP Error {ex}")
    except Exception as ex:
        raise Exception(f"Could not write mapping for {index}: {ex}")


class ES:
    def __init__(self, index_fragment: str):
        self.index = f"{BASE_INDEX}-{index_fragment}"
        es_create_index(self.index)

    def post_record(self, record):
        es_url = f"{BASE_URL}/{self.index}/_doc"
        # print(f"putting: [{es_url}]: {record}")
        try:
            es_http('POST', es_url, record)
        except Exception as ex:
            print(f"Problem inserting {record['location_desc']}")
