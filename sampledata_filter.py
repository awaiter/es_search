#!/usr/bin/env python
# -*- coding:utf-8 -*-


from elasticsearch import Elasticsearch
import datetime
import argparse

'''
sampleData filtering: de-weighting according to a certain variable
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

filter_yesterday = (datetime.datetime.now() + datetime.timedelta(days=0)).strftime("%Y-%m-%d")
filter_today = datetime.datetime.now().strftime("%Y-%m-%d")

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
    # set get_size
    size=200,
    body={
        'query': {
            'range': {
                '@timestamp': {
                    # 修改时间参数, 后面设置时间
                    "gt": "{date}T13:00:00||-8h".format(date=filter_today),
                    "lt": "{date}T13:59:59||-8h".format(date=filter_today),
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


def filter_v3(doc_type="snmp", filter_key="MachineIP"):
    tmp = []
    filter_value = []
    for x in page['hits']['hits']:
        if x['_source'][doc_type][filter_key] not in tmp:
            tmp.append(x['_source'][doc_type][filter_key])
            filter_value.append(x['_source'][doc_type])
    return filter_value


print filter_v3(doc_type=query_type, filter_key=query_key)
