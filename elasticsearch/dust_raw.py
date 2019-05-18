# -*- coding: utf-8 -*-
import elasticsearch
import requests
import urllib
import json
import time
import dust_station_data_insert
from elasticsearch import helpers


SERVICE_KEY = ''

# Apis
# 측정별 최종확정 농도 조회
GET_STATION_DAILY_DUST = 'http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnStatsSvc/getMsrstnAcctoLastDcsnDnsty'

es = elasticsearch.Elasticsearch('localhost:9200')


# index 생성
def create_dust_raw():
    es.indices.create(
        index='dust_raw',
        body={
            'mappings': {
                '_doc': {
                    'properties': {
                        'data_time': {
                            'type': 'date',
                            'format': 'yyyy-MM-dd'
                        },
                        'pm10_avg': {
                            'type': 'integer'
                        },
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
def insert_dust_raw(num_of_rows):
    page_no = 1
    base_url = GET_STATION_DAILY_DUST
    search_condition = 'DAILY'
    no_data_cnt = 0
    no_station_cnt = 0

    dust_measure_stations = dust_station_data_insert.search_all_dust_measure_station(500)
    cnt_down = len(dust_measure_stations)

    for dust_measure_station in dust_measure_stations:
        province = dust_measure_station['_source']['province']
        station = dust_measure_station['_source']['station']
        province_location = dust_measure_station['_source']['province_location']
        station_location = dust_measure_station['_source']['station_location']

        response = requests.get(
            base_url + '?'
            'serviceKey=' + SERVICE_KEY + '&'
            'numOfRows=' + str(num_of_rows) + '&'
            'pageNo=' + str(page_no) + '&'
            'stationName=' + station + '&'
            'searchCondition=' + search_condition + '&'
            '_returnType=json'
        )

        result = response.json()

        if int(result['parm']['numOfRows']) < result['totalCount']:
            print('Raise num of rows!!!; params: ' + result['parm'] + ', total count: ' + result['totalCount'])
            continue

        docs = []

        for elem in result['list']:
            data_time = elem['dataTime']
            try:
                pm10_avg = int(elem['pm10Avg'])
            except ValueError as e:
                # print('pm10 Value error: ', e, station, data_time)
                pm10_avg = None
                no_data_cnt += 1
                continue

            body = {
                'data_time': data_time,
                'pm10_avg': pm10_avg,
                'province': province,
                'station': station,
                'province_location': province_location,
                'station_location': station_location
            }

            docs.append({
                '_index': 'dust_raw',
                '_type': '_doc',
                '_source': body
            })
            # es.index(index="dust_raw", body=body)

        # es.index(index="dust_raw", body=body)
        # res = es.bulk(index="dust_raw", body=docs, doc_type='_doc')
        # res = helpers.bulk(es, docs, chunk_size=num_of_rows)

        if docs:
            res = helpers.bulk(es, docs)
            print(province, station, res, cnt_down)
        else:
            print('not found!: ', province, station, cnt_down)
            no_station_cnt += 1

        cnt_down -= 1

    return {'no_data_cnt': no_data_cnt, 'no_station_cnt': no_station_cnt}


def get_province_pm10_avg(province, data_time):
    res = es.search(
        index='dust_raw',
        body={
            'query': {
                'bool': {
                    'must': [
                        {
                            'match': {
                                'province': province
                            }
                        },
                        {
                            'match': {
                                'data_time': data_time
                            }
                        }
                    ]
                }
            },
            'aggs': {
                'province_pm10_avg': {
                    'avg': {
                        'field': 'pm10_avg'
                    }
                }
            }
        }
    )
    # print("Got %d Hits:" % res['hits']['total'])

    return res['aggregations']['province_pm10_avg']['value']


# create_dust_raw()
# res = insert_dust_raw(5000)
# print(res)
