# -*- coding: utf-8 -*-
import elasticsearch
import json
import common

es = elasticsearch.Elasticsearch("localhost:9200")

def search_dust_aggregation(province):
    date_list = {} # 날짜 리스트

    res = es.search(
    index='dust_aggregation',
    size = 10000,
    scroll = "2m",
    body={
        'query': {
            'bool': {
                'must': [
                    {
                        'match': {
                            'province': province
                            }
                        }
                    ]
                }
            }
        }
    )

    scroll_id = res["_scroll_id"]
    scroll_size = res["hits"]["total"]["value"]

    for data in res["hits"]["hits"]:
        date_list[data["_source"]["data_time"]] = data["_source"]["pm10_avg"]

    while(scroll_size > 0):
        res = es.scroll(scroll_id=scroll_id, scroll="2m")
        scroll_id = res["_scroll_id"]
        scroll_size = len(res["hits"]["hits"])
        for data in res["hits"]["hits"]:
            date_list[data["_source"]["data_time"]] = data["_source"]["pm10_avg"]
    
    return date_list


def export_data():
    dictionary = {}

    for province in common.COORDINATE_DICT.keys():
        dictionary[province] = search_dust_aggregation(province)

        with open("./data.json", "w", encoding='UTF-8-sig') as fp:
            fp.write(json.dumps(dictionary, ensure_ascii=False))
        
        
export_data()


