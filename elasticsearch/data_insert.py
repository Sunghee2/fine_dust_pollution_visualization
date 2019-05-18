#-*- coding: utf-8 -*-

import elasticsearch
import requests
import urllib
import json
from xml.etree import ElementTree

num_of_rows = 10
page_no = 1
station_name = "종로구"
search_condition = "DAILY"
service_key = "service key"
base_url = "http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnStatsSvc/getMsrstnAcctoLastDcsnDnsty"

# index 생성
def createIndex():
    es.indices.create(
        index="dust", # index명
        body={
            "mappings": {
                "properties": {
                    # column
                    "date": {
                        "type": "date"
                    },
                    "station": {
                        "type": "text"
                    },
                    "value": {
                        "type": "integer"
                    }
                }
            }
        }
    )

# data 삽입
def insertData():
    r = requests.get(base_url, params={
        "numOfRows": num_of_rows,
        "pageNo": page_no,
        "stationName": station_name,
        "searchCondition": search_condition,
        "serviceKey": urllib.unquote(service_key)
    })

    # print(r.url)
    # print(r.text)

    root = ElementTree.fromstring(r.content)
    print(root.findall("./body/items/item"))

    for item in root.findall("./body/items/item"):
        date = item.find("dataTime").text
        value = item.find("pm10Avg").text.strip()
        print(date, value)

        body = {
            "date": date,
            "station": station_name,
            "value": value
        }

        res = es.index(index="dust", body=body)
        print(res)

# 전체 data 조회
def searchAll():
    res = es.search(
        index = "dust",
        body = {
            "query": {"match_all": {}}
        }
    )

    print(json.dumps(res))

# value 조회
def searchFilter(tag, value):
    res = es.search(
        index = "dust",
        body = {
            "query": {
                "match": {
                    tag: value
                }
            }
        }
    )

    print(json.dumps(res))

# 객체 생성
es = elasticsearch.Elasticsearch("localhost:9200")
# print(es.cat.indices())

# createIndex()
# insertData()
# searchAll()
searchFilter("date", "2006-01-03")