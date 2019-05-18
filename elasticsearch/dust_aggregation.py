# -*- coding: utf-8 -*-
import elasticsearch
from elasticsearch import helpers
import requests
import urllib
import json
import time
from datetime import date
from dateutil.rrule import rrule, DAILY
import dust_raw
# from .common import COORDINATE_DICT
import common

es = elasticsearch.Elasticsearch('localhost:9200')


# index 생성
def create_dust_aggregation():
    es.indices.create(
        index='dust_aggregation',
        body={
            'mappings': {
                '_doc': {
                    'properties': {
                        'data_time': {
                            'type': 'date',
                            'format': 'yyyy-MM-dd'
                        },
                        'province': {
                            'type': 'keyword'
                        },
                        'province_location': {
                            'type': 'geo_point'
                        },
                        'pm10_avg': {
                            'type': 'float'
                        }
                    }
                }
            }
        }
    )


def insert_dust_aggregation():
    for province in common.COORDINATE_DICT.keys():
        docs = []
        start_date = date(2006, 1, 1)
        end_date = date(2017, 12, 31)

        for dt in rrule(DAILY, dtstart=start_date, until=end_date):
            dt_to_str = dt.strftime('%Y-%m-%d')
            province_pm10_avg = dust_raw.get_province_pm10_avg(province, dt_to_str)

            if province_pm10_avg:
                body = {
                    'data_time': dt_to_str,
                    'province': province,
                    'province_location': {
                        'lat': common.COORDINATE_DICT[province]['latitude'],
                        'lon': common.COORDINATE_DICT[province]['longitude']
                    },
                    'pm10_avg': province_pm10_avg
                }

                docs.append({
                    '_index': 'dust_aggregation',
                    '_type': '_doc',
                    '_source': body
                })

        if docs:
            res = helpers.bulk(es, docs)
            print(province, res)
        else:
            print('not found!: ', province)


# create_dust_aggregation()
# insert_dust_aggregation()
