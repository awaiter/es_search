#!/usr/bin/env python
# -*- coding:utf-8 -*-


from elasticsearch import Elasticsearch
import json
import datetime
import argparse

'''
Data filtering: de-weighting according to a certain variable
default:
    get_data from 'now-h' or set by yourself
'''

default_ip = "192.168.10.201"
default_index = "cc-gossip-snmp-4a859fff6e5c4521aab18*"
default_type = "snmp"
default_key = "MachineIP"

parser = argparse.ArgumentParser(description='filter the value of es.')
# ip
parser.add_argument('-i', '--ip', type=str, default=default_ip, help="es's ip.")
# port
parser.add_argument('-p', '--port', type=int, default=9200, help="es's port, default 9200.")
# index
parser.add_argument('-d', '--index', type=str, default=default_index, help="es's index.")
# doc_type
parser.add_argument('-t', '--type', type=str, default=default_type, help="es's type.")
# key
parser.add_argument('-k', '--key', type=str, default=default_key, help="query key of the value.")
args = parser.parse_args()
es_host = args.ip
es_port = args.port
query_index = args.index
query_type = args.type
query_key = args.key

# set filter now -N*hours, default N=1
less_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
file_name = "%s_%s.json" % ((datetime.datetime.now() + datetime.timedelta(hours=-1)).strftime("%y%m%d%H%M"),
                            datetime.datetime.now().strftime("%y%m%d%H%M"))
greater_time = (datetime.datetime.now() + datetime.timedelta(hours=-1)).strftime("%Y-%m-%dT%H:%M:%S")

es = Elasticsearch([
    {'host': es_host,
     'port': es_port}
])

page = es.search(
    index=query_index,
    # type
    doc_type=query_type,
    scroll='2m',
    # search_type='',
    # 过滤取得的数值
    size=200,
    body={
        'query': {
            'range': {
                '@timestamp': {
                    # 修改时间参数, 后面设置时间
                    # "gt": "now-1h"
                    'gt': "%s||-8h" % greater_time,
                    'lt': "%s||-8h" % less_time,
                    # "gt": "{date}T08:30:00||-8h".format(date=filter_date),
                    # "lt": "{date}T09:30:59||-8h".format(date=filter_date)
                }
            }
        }
    }
)


def filter_key_value(doc_type="snmp", filter_key="MachineIP"):
    tmp = []
    for x in page['hits']['hits']:
        if x['_source'][doc_type][filter_key] not in tmp:
            tmp.append(x['_source'][doc_type][filter_key])
    return tmp


def filter_result(doc_type="snmp", filter_key="MachineIP"):
    tmp = []
    filter_value = []
    for x in page['hits']['hits']:
        if x['_source'][doc_type][filter_key] not in tmp:
            tmp.append(x['_source'][doc_type][filter_key])
            filter_value.append(x)
    return filter_value


with open(file_name, "w") as f:
    f.write(json.dumps(filter_result(doc_type=query_type, filter_key=query_key), indent=2))
