#-*- coding: utf-8 -*-

import elasticsearch
import requests
import urllib.parse
import json
from xml.etree import ElementTree

num_of_rows = 400
page_no = 1
station_name = "창원"
search_condition = "DAILY"
addr_station = "경남"
service_key = 
base_url = "http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnStatsSvc/getMsrstnAcctoLastDcsnDnsty"

# index 생성
def createIndex_station_info():
    es.indices.create(
        index="station_info", # index명
        body={
            "mappings": {
                "properties": {
                    # column
                    "station": {
                        "type": "keyword"
                    },
                    "province": {
                        "type": "keyword"
                    },
                    "latitude": {
                        "type": "geo_point"
                    },
                    "longitude": {
                        "type": "geo_point"
                    }
                }
            }
        }
    )

# data 삽입
def insertData_station_info():
    r = requests.get(base_url, params={
        "numOfRows": num_of_rows,
        "pageNo": page_no,
        "addr": addr_station,
        "stationName": station_name,
        "serviceKey": urllib.parse.unquote(service_key)   
    })

    # print(r.url)
    # print(r.text)


    province_dic = {
        '서울': {"latitude": 37.5408, "longitude": 126.994},
        '부산': {"latitude": 35.1928, "longitude": 129.082},
        '대구': {"latitude": 35.9034, "longitude": 128.631},
        '인천': {"latitude": 37.4983, "longitude": 126.507},
        '광주': {"latitude": 35.1989, "longitude": 126.929},
        '대전': {"latitude": 36.3563, "longitude": 127.401},
        '울산': {"latitude": 35.4997, "longitude": 129.232},
        '경기': {"latitude": 37.2562, "longitude": 127.205},
        '강원': {"latitude": 37.7951, "longitude": 128.22},
        '충북': {"latitude": 36.7476, "longitude": 127.668},
        '충남': {"latitude": 36.4602, "longitude": 126.874},
        '전북': {"latitude": 35.679, "longitude": 127.081},
        '전남': {"latitude": 34.8166, "longitude": 126.884},
        '경북': {"latitude": 36.3396, "longitude": 128.708},
        '경남': {"latitude": 35.3192, "longitude": 128.217},
        '제주': {"latitude": 33.3741, "longitude": 126.557}
        }

    
    for keyword in province_dic:
        print(keyword)
        if keyword in addr_station:
            province_name = keyword
            latitude_data = province_dic[keyword]["latitude"]
            longitude_data = province_dic[keyword]["longitude"]
            break

    if searchNotExist(station_name):
        body = {
        "station": station_name,
        "province": province_name, 
        "latitude": latitude_data,
        "longitude": longitude_data
        }
        res = es.index(index="station_info", body=body)


# 전체 data 조회
def searchAll():
    res = es.search(
        index = "station_info",
        body = {
            "query": {"match_all": {}}
        }
    )

    print(json.dumps(res))

def searchNotExist(tag):
    res = es.search(
        index = "station_info",
        body = {
            "query":{
                "match": {
                    "station" : tag
                }
            }
        }
    )
    
    if not res:
        return False
    else:
        return True


# value 조회
def searchFilter(tag, value):
    res = es.search(
        index = "station_info",
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

#createIndex_station_info()
insertData_station_info()
searchAll()
#searchFilter("date", "2006-01-03")
