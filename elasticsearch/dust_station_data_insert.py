# -*- coding: utf-8 -*-
import elasticsearch
import requests
import urllib
import json
import time
import common

SERVICE_KEY = ''

# Apis
# 측정소 목록 조회
GET_MEASURE_STATION_LIST = 'http://openapi.airkorea.or.kr/openapi/services/rest/MsrstnInfoInqireSvc/getMsrstnList'

es = elasticsearch.Elasticsearch('localhost:9200')


# index 생성
def create_dust_measure_station():
    es.indices.create(
        index='dust_measure_station',
        body={
            'mappings': {
                '_doc': {
                    'properties': {
                        'province': {
                            'type': 'keyword'
                        },
                        'station': {
                            'type': 'keyword'
                        },
                        'province_location': {
                            'type': 'geo_point'
                        },
                        'station_location': {
                            'type': 'geo_point'
                        }
                    }
                }
            }
        }
    )


# data 삽입
def insert_dust_measure_station():
    num_of_rows = 100
    page_no = 1
    base_url = GET_MEASURE_STATION_LIST
    _return_type = "json"
    cnt = 1

    for province in common.COORDINATE_DICT.keys():
        # 단시간에 10번 이상 요청을 날리면 api 접근을 몇 분간 막는 것 같습니다. 5초도 막히면 10초로 늘려보세요
        if cnt > 9:
            cnt = 1
            print('Now sleeping for 5 seconds')
            for i in range(5, 0, -1):
                time.sleep(1)
                print(i)

        # response = requests.get(base_url, params={
        #     "serviceKey": urllib.parse.unquote(SERVICE_KEY),
        #     "numOfRows": num_of_rows,
        #     "pageNo": page_no,
        #     "addr": addr,
        #     "_returnType": _return_type
        # })
        response = requests.get(
            base_url + '?'
            'serviceKey=' + SERVICE_KEY + '&'
            'numOfRows=' + str(num_of_rows) + '&'
            'pageNo=' + str(page_no) + '&'
            'addr=' + province + '&'
            '_returnType=json'
        )

        print('request cnt: ', province, cnt)

        result = response.json()
        cnt += 1

        if int(result['parm']['numOfRows']) < result['totalCount']:
            print('Raise num of rows!!!; params: ' + result['parm'] + ', total count: ' + result['totalCount'])
            continue

        for elem in result['list']:
            station = elem['stationName']
            province_lat = common.COORDINATE_DICT[province]['latitude']
            province_lon = common.COORDINATE_DICT[province]['longitude']

            try:
                station_lat = float(elem['dmX'])
            except ValueError as e:
                print('Value error: ', e)
                print(province, elem['stationName'])
                print('dm_x: ', elem['dmX'])
                station_lat = None

            try:
                station_lon = float(elem['dmY'])
            except ValueError as e:
                print('Value error: ', e)
                print(province, elem['stationName'])
                print('dm_y: ', elem['dmY'])
                station_lon = None

            station_location = {
                'lat': station_lat,
                'lon': station_lon
            }

            if station_lat is None or station_lon is None:
                station_location = None

            body = {
                'province': province,
                'station': station,
                'province_location': {
                    "lat": province_lat,
                    'lon': province_lon
                },
                'station_location': station_location
            }

            es.index(index="dust_measure_station", body=body)
        print(province + ' Success!')


# 전체 data 조회
def search_all_dust_measure_station(size=10):
    res = es.search(
        index='dust_measure_station',
        body={
            'size': size,
            'query': {'match_all': {}}
        }
    )
    print("Got %d Hits:" % res['hits']['total'])
    # for hit in res['hits']['hits']:
    #     print("%(province)s %(station)s: %(province_location)s" % hit["_source"])
    # print(json.dumps(res, ensure_ascii=False, indent=4))

    return res['hits']['hits']


# value 조회
def search_filter(tag, value):
    res = es.search(
        index='dust_measure_station',
        body={
            'query': {
                'match': {
                    tag: value
                }
            }
        }
    )
    print(json.dumps(res))


def delete_all():
    res = es.delete_by_query(
        index='dust_measure_station',
        body={
            'query': {
                'match_all': {

                }
            }
        }
    )
    # res = es.delete_by_query(index='dust_measure_station', body={'query': {'match': {'province': '경기'}}})
    print(json.dumps(res))


# create_dust_measure_station()
# insert_dust_measure_station()
