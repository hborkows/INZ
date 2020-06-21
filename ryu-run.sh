#! /bin/bash

cd docker/elk
sudo docker-compose down
sudo docker-compose up -d
cd ../..

cd ddos-detector
ryu-manager --verbose monitor_13.py

