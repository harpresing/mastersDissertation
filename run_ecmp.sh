#!/bin/bash

# cleanup previous executions
./cleanup.sh
mkdir /tmp/iperf
killall -9 python2.7
sudo mn -c
sleep 5

~/pox/pox.py controllers.riplpox --topo=ft,4 --routing=hashed --mode=reactive &
sleep 2
sudo python LaunchExperiment.py
sleep 6