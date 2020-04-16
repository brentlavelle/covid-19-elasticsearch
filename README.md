# covid-19-elasticsearch
Data importers for Covid-19 data so that people can visualize public data sets in Kibana - Python edition

## Install
* Run Elasticsearch locally on port 9200 it should be version 7. I'm using 7.6.2
* Make sure you have a new Python I used 3.8.2 pyenv is a great way of doing that
* clone this repo
* clone the JHU CSSE data COVID19 repo https://github.com/CSSEGISandData/COVID-19
* run this program and give it the path to one of the csse_covid_19_daily report files in the JHU CSSE files
```shell script
python read_ccse_csv_data.py ../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/04-13-2020.csv
```
