# -*- coding: utf-8 -*-

import elasticsearch
import requests
import urllib
import json
import time
import datetime


startingDate = "2006-01-01"
endingDate = "2017-12-31"

coordinate_dict = {
    '서울': {'latitude': 37.5408, 'longitude': 126.994},
    '부산': {'latitude': 35.1928, 'longitude': 129.082},
    '대구': {'latitude': 35.9034, 'longitude': 128.631},
    '인천': {'latitude': 37.4983, 'longitude': 126.507},
    '광주': {'latitude': 35.1989, 'longitude': 126.929},
    '대전': {'latitude': 36.3563, 'longitude': 127.401},
    '울산': {'latitude': 35.4997, 'longitude': 129.232},
    '경기': {'latitude': 37.2562, 'longitude': 127.205},
    '강원': {'latitude': 37.7951, 'longitude': 128.22},
    '충북': {'latitude': 36.7476, 'longitude': 127.668},
    '충남': {'latitude': 36.4602, 'longitude': 126.874},
    '전북': {'latitude': 35.679, 'longitude': 127.081},
    '전남': {'latitude': 34.8166, 'longitude': 126.884},
    '경북': {'latitude': 36.3396, 'longitude': 128.708},
    '경남': {'latitude': 35.3192, 'longitude': 128.217},
    '제주': {'latitude': 33.3741, 'longitude': 126.557}
}

# dust_aggre index 생성
def createIndex():
    es.indices.create(
        index="dust_aggre",
        body={
            "mappings": {
                "properties": {
                    "dataTime": {
                        "type": "date"
                    },
                    "average": {
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

#데이터 입력
def insertDataAvg(date, dust_avg, province):


    body = {
        "dataTime" : date,
        "average" : dust_avg,
        "province" : province,
        "location" : {
            "lat" : coordinate_dict[province]["latitude"]
            "lon" : coordinate_dict[province]["longitude"]
            }
        }

    es.index(index="dust_aggre", body=body)


#시작 날짜와 끝 날짜를 인자로 전달합니다.
#주어진 기간동안의 pm10Avg 평균을 searchRawIndex를 통해 가져온 후 insertDataAvg를 호출합니다.
def searchByDate(start, end):

        diff = end - start
        diff2 = diff.days

        for i in range (0, diff2, 1)
            date = start + datetime.timedelta(days=1)
            for province in coordinate_dict.keys():
                dust_avg = searchRawIndex(date, province)
                insertDataAvg(date, dust_avg, province)


        

#dust_raw index로부터 날짜와 시/도명이 일치하는 pm10Avg 값을 가져옵니다.
#같은 날짜에 해당하는 pm10Avg 값을 더해 평균을 내 return 합니다.
def searchRawIndex(date, province):
        date = index.strftime("%Y-%m-%d")
        res = es.search(
            index='dust_raw',
            body={
                'query': {
                    'match': {
                        'date' : date
                        'province' : province
                        }
                    }
                }
            )

        total = res[_shards][total]
        dust_total = 0
        
        for i in total:
            dust_total += res[_source][i][pm10Avg] #이부분 고치기: 날짜에 해당하는 데이터 개수만큼 pm10Avg 값을 받아와 합을 계산


        dust_average = dust_total / i

        return dust_average
        
            



es = elasticsearch.Elasticsearch('localhost:9200')

startingDate = datetime.datetime(2006, 1, 1)
endingDate = datetime.datetime(2017, 12, 31)

searchByDate(satrtingDate, endingDate)
