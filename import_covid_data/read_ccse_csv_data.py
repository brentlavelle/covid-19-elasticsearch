from es import ES
import os
import csv
import datetime
import re

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

    return new


def read_csv(file):
    file_base = os.path.basename(file)
    if match := re.match('^(\d\d?)-(\d\d?)-(\d{4})', file_base):
        index_fragment = f"{match.group(3)}-{match.group(1)}-{match.group(2)}"
    else:
        index_fragment = f"file={file_base}"

    es = ES(index_fragment)

    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            timestamp = datetime.datetime.strptime(row['Last_Update'], '%Y-%m-%d %H:%M:%S')
            row['Last_Update'] = timestamp
            # es.post_record(convert_record(row))
            es.bulk_add(convert_record(row))
            print(f"{int(row['Active']): 10} active cases at: {row['Combined_Key']}")
    es.bulk_write()

