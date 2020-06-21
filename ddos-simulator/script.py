import sys, random
import logging
logging.getLogger("scapy.runtime").setLevel(logging.ERROR)
import scapy
from scapy.all import *
from scapy.layers import inet
import time
import os
import binascii
import json
import ipaddress

n_threads = 3
n_minutes = 15

def generate_botnet_ips(subnet_list, n_bots):
	ips = []
	for subnet in subnet_list:
		ips += [str(ip) for ip in ipaddress.IPv4Network(subnet)]

	return random.sample(ips, n_bots)

def read_ip_list(filename):
	with open(filename) as json_file:
		data = json.load(json_file)
		
	return data

def get_random_mac():
	return binascii.hexlify(os.urandom(12), ':')

def sendPacket(src_ip, dst_ip, src_mac, dst_mac, dst_port):
	data = b'1'
	packet = inet.Ether(src=src_mac, dst=dst_mac)/inet.IP(src=src_ip, dst=dst_ip)/inet.TCP(dport=dst_port)/Raw(load=data)
	sendp(packet)
	#print(packet.show())

#----------------------------------------------------------------------------------------------

# With threading
t_end = time.time() + 60 * n_minutes
target_ips = read_ip_list('target_ips.json')
source_subnets = read_ip_list('source_ips.json')
source_ips = generate_botnet_ips(source_subnets, 7)

print('Starting')

while time.time() < t_end:
	sendPacket(src_ip=random.choice(source_ips), dst_ip=random.choice(target_ips), src_mac=get_random_mac(), dst_mac=get_random_mac(), dst_port=random.choice(range(1, 49151)))

print('Finished')
