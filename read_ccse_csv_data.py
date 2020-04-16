import sys
import csv
import urllib.request
import datetime
import json

HOST = "localhost"
PORT = 9200
BASE_INDEX = "covid_ccse-"


def put_record(record, timestamp):
    header = {"Content-Type": "application/json"}
    es_url = f"http://{HOST}:{PORT}/{BASE_INDEX}{timestamp.year}-{timestamp.month:02}/_doc"
    # print(f"putting: [{es_url}]: {record}")
    request = urllib.request.Request(url=es_url, data=json.dumps(record).encode('utf8'), method='POST', headers=header)
    try:
        f = urllib.request.urlopen(request)
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
    if "Lat" in record and "Long_" in record and len(record["Lat"]) >0 and len(record["Long_"]) > 0:
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
    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            timestamp = datetime.datetime.strptime(row['Last_Update'], '%Y-%m-%d %H:%M:%S')
            row['Last_Update'] = timestamp
            if row['Country_Region'] == 'Canada':
                print("Whats up with Canada?")
            put_record(convert_record(row), timestamp)
            # print(f"location:  {row['Combined_Key']} has {row['Active']} active cases")


if __name__ == '__main__':
    def main():
        data = '../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/04-13-2020.csv'
        if len(sys.argv) > 1:
            data = sys.argv[1]
        read_csv(data)

    main()


# PUT _template/covid_ccse
MAPPING = {
  "index_patterns": ["covid_ccse-*"],
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "version": 1,
  "mappings": {
    "_source": {
      "enabled": True
    },
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
      "location_desc": {"type": "text"}
    }
  }
}

#FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key
#45001,Abbeville,South Carolina,US,2020-04-13 23:07:54,34.22333378,-82.46170658,9,0,0,9,"Abbeville, South Carolina, US"
#22001,Acadia,Louisiana,US,2020-04-13 23:07:54,30.295064899999996,-92.41419698,101,5,0,96,"Acadia, Louisiana, US"
#51001,Accomack,Virginia,US,2020-04-13 23:07:54,37.76707161,-75.63234615,15,0,0,15,"Accomack, Virginia, US"
