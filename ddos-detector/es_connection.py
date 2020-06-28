from elasticsearch import Elasticsearch
import requests
import json
import pandas as pd 
from datetime import datetime
from pandasticsearch import Select
from ryu.lib.packet import packet, ethernet, ipv4, tcp

class ESConnection:
    def __init__(self, es_host, logstash_host):
        self._es_host = es_host
        self._logstash_host = logstash_host
        self._elasticsearch = Elasticsearch([self._es_host])
    
    def send_packet(self, packet, target_index):
        eth_pkt = packet.get_protocol(ethernet.ethernet)
        tcp_pkt = packet.get_protocol(tcp.tcp)
        ipv4_pkt = packet.get_protocol(ipv4.ipv4)

        packet_dict = {
            'index_name': target_index,
            'src_ip': ipv4_pkt.src,
            'dst_ip': ipv4_pkt.dst,
            'src_port': tcp_pkt.src_port,
            'dst_port': tcp_pkt.dst_port,
            'src_mac': eth_pkt.src,
            'dst_mac': eth_pkt.dst,
            'raw': str(packet.protocols[-1])
        }
        print('Sending: ' + str(packet_dict))

        requests.post(url=self._logstash_host, json=packet_dict, verify=False)

    def _unix_time_millis(self, tstamp):
        epoch = datetime.utcfromtimestamp(0)
        dt = datetime.strptime(tstamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        return (dt - epoch).total_seconds() * 1000

    def _es_query(self, index, query, sort, search_after, max_size):
        tmp = self._elasticsearch.search(
            index=index,
            body={
                "size": max_size,
                "query": query,
                "sort": sort,
                "search_after": search_after
            }
        )
        return tmp

    def index_data_to_df(self, index_name, start_time, end_time, field_list):
        query = {
            "range": {
                "@timestamp": {
                    "gte": start_time,
                    "lte": end_time
                }
            }
        }
        sort = {
            "@timestamp": {
                "order": "asc"
            },
            "_id": {
                "order": "asc"
            }
        }
        max_size = 5000
        search_after = [0,0]
        pandas_df = pd.DataFrame()

        while True:
            result_dict = self._es_query(index_name, query, sort, search_after, max_size)
            if len(result_dict['hits']['hits']) == 0:
                break

            tmp = Select.from_dict(result_dict).to_pandas()
            pandas_df = pandas_df.append(tmp[field_list])

            search_after = [
                self._unix_time_millis(pandas_df['@timestamp'].iloc[-1]),
                pandas_df['_id'].iloc[-1]
            ]
        
        pandas_df.to_csv('input_ryu.csv')
        return pandas_df
