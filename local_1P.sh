#!/bin/sh
rm -fr *.out  # remove any existing out files

python3 DiscoveryAppln.py -P 1 -S 1 > discovery.out 2>&1 &
python3 PublisherAppln.py -T 5 -n pub1 -p 5570 > pub1.out 2>&1 &

