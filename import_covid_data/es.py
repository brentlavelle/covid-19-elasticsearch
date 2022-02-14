import urllib.request
from urllib.error import HTTPError
import json

BASE_URL = "http://localhost:9200"
BASE_INDEX = "covid_ccse-"


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

    # _template defaults commented out
    settings = {
        # "index_patterns": ["covid_ccse-*"],
        # "version": 1,
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
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
