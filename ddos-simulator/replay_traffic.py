import sys, random
import logging
logging.getLogger("scapy.runtime").setLevel(logging.ERROR)
import scapy
from scapy.all import *
from scapy.layers import inet
import time
import os
import json
import ipaddress
import pandas as pd
from datetime import datetime, timedelta
import sched


reference_timestamp = None

def calculate_timestamp_offset(timestamp):
	global reference_timestamp
	timestamp = datetime.strptime(timestamp, '%d/%m/%Y%H:%M:%S')
	delta = timestamp - reference_timestamp 
	return delta.total_seconds()

def read_ip_list(filename):
	with open(filename) as json_file:
		data = json.load(json_file)
		
	return data

def get_random_mac():
	return binascii.hexlify(os.urandom(12), ':')

def sendPacket(src_ip, dst_ip, src_mac, dst_mac, dst_port):
	data = b'0'
	packet = inet.Ether(src=src_mac, dst=dst_mac)/inet.IP(src=src_ip, dst=dst_ip)/inet.TCP(dport=dst_port)/Raw(load=data)
	print(packet.show())
	sendp(packet)
	#

def replay_traffic(dataset, target_ips):
	sch = sched.scheduler(time.time, time.sleep)
	for index, row in dataset.iterrows():
		rand = random.choice(target_ips)
		sch.enter(row['timestamp_offset'], priority=1, action=sendPacket, argument=(row['Source.IP'], rand['ip'], row['Source.MAC'], rand['mac'], row['Destination.Port']))
		print('Scheduling: ' + str(row['Timestamp']))
	#print('Estimated end time: ' + str(datetime.strptime(row['Timestamp'], '%d/%m/%Y%H:%M:%S') + row['timestamp_offset']))
	sch.run()

#----------------------------------------------------------------------------------------------
print('Starting')
target_ips = read_ip_list('norm_target_ip.json')
dataset_df = pd.read_csv('cut_dataset.csv')
reference_timestamp = datetime.now()
print('Calculating timestamps')
dataset_df['timestamp_offset'] = dataset_df['Timestamp'].apply(calculate_timestamp_offset)

print('Starting replay')

replay_traffic(dataset=dataset_df, target_ips=target_ips)

print('Finished')
