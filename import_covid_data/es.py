import urllib.request
from urllib.error import HTTPError
import json

BASE_URL = "http://localhost:9200"
BASE_INDEX = "covid_ccse"

# Doc counts are around 3100 per day with about 650KB so 1 shard is good
SHARDS = 1
# Loading data locally into a 1 node cluster so no replicas, if I lose data I reload it. Backed up in Github.com
REPLICAS = 0

# How many actions to load up in a bulk operation
BULK_SIZE = 2000


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
        self.index_url = f"{BASE_INDEX}-{index_fragment}"
        # self.index = index_fragment
        es_create_index(self.index_url)
        self.payload = ""
        self.size = 0

    def post_record(self, record):
        es_url = f"{BASE_URL}/{self.index_url}/_doc"
        try:
            es_http('POST', es_url, record)
        except Exception as ex:
            print(f"Problem inserting {record['location_desc']}: {ex}")

    def bulk_add(self, record: dict):

        self.payload += json.dumps({"index": {}}) + "\n" + json.dumps(record) + "\n"
        self.size += 1
        if self.size >= BULK_SIZE:
            self.bulk_write()

    def bulk_write(self):
        header = {"Content-Type": "application/x-ndjson"}
        url = f"{BASE_URL}/{self.index_url}/_bulk"
        request = urllib.request.Request(url=url, data=self.payload.encode("utf-8"), method="POST", headers=header)
        urllib.request.urlopen(request)
        # FIXME, this assumes everything goes as planned
        self.payload = ""
        self.size = 0


