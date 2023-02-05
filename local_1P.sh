#!/bin/sh
rm -fr *.out  # remove any existing out files

python3 DiscoveryAppln.py -P 1 -S 1 > log_discovery.out 2>&1 &
python3 PublisherAppln.py -T 9 -n pub1 -p 5570 > log_pub1.out 2>&1 &
python3 SubscriberAppln.py -T 2 -n sub1 > log_sub1.out 2>&1 &

