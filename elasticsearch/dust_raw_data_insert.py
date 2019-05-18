#-*- coding: utf-8 -*-

import elasticsearch
import requests
import urllib
import json

page_no = 1
search_condition = "DAILY"
service_key = ""
base_url = "http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnStatsSvc/getMsrstnAcctoLastDcsnDnsty"

def createIndex():
    es.indices.create(
        index="dust_raw",
        body={
            "mappings": {
                "properties": {
                    "dataTime": {
                        "type": "date"
                    },
                    "station": {
                        "type": "keyword"
                    },
                    "pm10Avg": {
                        "type": "integer"
                    },
                    "province": {
                        "type": "keyword"
                    },
                    "location": {
                        "type": "geo_point"
                    }
                }
            }
        }
    )

# api를 호출하여 dust_raw index에 데이터를 저장하는 함수입니다.
# station은 dust_measure_station에서 측정소 정보를 받아온 것입니다.
# num_of_rows는 api 호출시 한 페이지에 나타날 행의 수를 의미합니다. 기본적으로 2006-01-01 ~ 2017-12-31인 4345를 설정하였습니다.
def insertData(station, num_of_rows):
    r = requests.get(base_url, params={
        "numOfRows": num_of_rows,
        "pageNo": page_no,
        "stationName": station["_source"]["station"],
        "searchCondition": search_condition,
        "serviceKey": urllib.parse.unquote(service_key),
        "_returnType": "json"
    })

    results = r.json()

    # api 호출했을 때의 보여지는 data보다 총 data 수가 큰 경우 num_of_rows를 수정하여 insertData를 다시 호출합니다.
    if(num_of_rows < results["totalCount"]):
        insertData(station, results["totalCount"])
        return None

    for result in results["list"]:
        # api에서 pm10Avg값이 없는 경우가 있어 처리해주었습니다.
        if result["pm10Avg"] is None:
            continue

        body = {
            "dataTime": result["dataTime"],
            "pm10Avg": result["pm10Avg"].strip(),
            "station": station["_source"]["station"],
            "province": station["_source"]["province"],
            "location": {
                "lat": station["_source"]["province_location"]["lat"],
                "lon": station["_source"]["province_location"]["lon"]
            }
        }

        res = es.index(index="dust_raw", body=body)
        if res["_shards"]["successful"] == 1:
            print("%s %s-%s 완료"%(result["dataTime"], station["_source"]["province"], station["_source"]["station"]))

# dust_measure_station index를 검색하여 각 측정소마다 insertData() 함수를 호출하는 함수입니다.
# 에러가 나서 중간에 종료될 경우를 고려하여 start_idx(시작위치)를 parameter로 넣었습니다.
def searchStationIndex(start_idx):
    stations = es.search(
        index = "dust_measure_station",
	size = 441,
        body = {
            "query": {"match_all": {}},
	    "from": start_idx
        }
    )

    for idx, station in enumerate(stations["hits"]["hits"]):
        print(station["_source"]["station"])
        insertData(station, 4345)
        print("{}번째 완료!".format(idx))


def searchFilter(tag, value):
    res = es.search(
        index = "dust_raw",
        size = 10000,
        body = {
            "query": {
                "match": {
                    tag: value
                }
            }
        }
    )

    print(json.dumps(res))

es = elasticsearch.Elasticsearch("localhost:9200")

createIndex()
searchStationIndex(0)
# searchFilter("station", "대안동")
