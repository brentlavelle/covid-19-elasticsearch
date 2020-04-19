import sys
import os
import csv
import urllib.request
from urllib.error import HTTPError
import datetime
import json
import re

HOST = "localhost"
PORT = 9200
BASE_INDEX = "covid_ccse-"


def es_http(method:str, url: str, record: dict = {}) -> None:
    header = {"Content-Type": "application/json"}
    if dict:
        request = urllib.request.Request(url=url, data=json.dumps(record).encode('utf8'), method=method, headers=header)
    else:
        request = urllib.request.Request(url=url, method=method, headers=header)
    urllib.request.urlopen(request)

def es_create_index(index: str):
    index_url = f"http://{HOST}:{PORT}/{BASE_INDEX}{index}"
    try:
        es_http('DELETE', index_url, {})
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
                "last_updated": {"type": "date"},
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


def post_record(record, index):
    es_url = f"http://{HOST}:{PORT}/{index}/_doc"
    # print(f"putting: [{es_url}]: {record}")
    try:
        es_http('POST', es_url, record)
    except Exception as ex:
        print(f"Problem inserting {record['location_desc']}")


MAP_FIELDS = {
    "FIPS": "fips",
    "Admin2": "county",
    "Province_State": "state",
    "Country_Region": "country",
    "Confirmed": "confirmed",
    "Deaths": "deaths",
    "Recovered": "recovered",
    "Active": "active",
    "Combined_Key": "location_desc",
}


def convert_record(record: dict) -> dict:
    new = {
        "last_update": record["Last_Update"].strftime('%Y-%m-%dT%H:%M:%S%z'),
    }
    if "Lat" in record and "Long_" in record and len(record["Lat"]) > 0 and len(record["Long_"]) > 0:
        new["location_point"] = f"{record['Lat']},{record['Long_']}"

    for source, destination in MAP_FIELDS.items():
        if source in record:
            if (len(record[source]) > 0):
                new[destination] = record[source]
            else:
                print(f"null in source {source} dest: {destination}")
        # if source in record and (len(record[source]) > 0):
        #     new[destination] = record[source]

    return new


def read_csv(file):
    file_base = os.path.basename(file)
    if match := re.match('^(\d\d?)-(\d\d?)-(\d{4})', file_base):
        index = f"{match.group(3)}-{match.group(1)}-{match.group(2)}"
    else:
        index = "X" + file_base

    es_create_index(index)

    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            timestamp = datetime.datetime.strptime(row['Last_Update'], '%Y-%m-%d %H:%M:%S')
            row['Last_Update'] = timestamp
            post_record(convert_record(row), index)
            # print(f"location:  {row['Combined_Key']} has {row['Active']} active cases")


if __name__ == '__main__':
    def main():
        data = '../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/04-13-2020.csv'
        if len(sys.argv) > 1:
            data = sys.argv[1]
        read_csv(data)


    main()
