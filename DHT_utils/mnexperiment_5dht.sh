h1 python3 DiscoveryAppln.py -n disc2 -j dht_5.json -p 5555 -P 5 -S 5 > disc2.out 2>&1 &
h3 python3 DiscoveryAppln.py -n disc4 -j dht_5.json -p 5555 -P 5 -S 5 > disc4.out 2>&1 &
h4 python3 DiscoveryAppln.py -n disc5 -j dht_5.json -p 5555 -P 5 -S 5 > disc5.out 2>&1 &
h19 python3 DiscoveryAppln.py -n disc1 -j dht_5.json -p 5555 -P 5 -S 5 > disc1.out 2>&1 &
h19 python3 DiscoveryAppln.py -n disc3 -j dht_5.json -p 5556 -P 5 -S 5 > disc3.out 2>&1 &
h2 python3 PublisherAppln.py -n pub3 -P 5 -S 5 -j dht_5.json -a 10.0.0.2 -p 7777 -T 7 -f 2 -i 3000 > pub3.out 2>&1 &
h2 python3 PublisherAppln.py -n pub5 -P 5 -S 5 -j dht_5.json -a 10.0.0.2 -p 7776 -T 5 -f 0.75 -i 3000 > pub5.out 2>&1 &
h10 python3 PublisherAppln.py -n pub1 -P 5 -S 5 -j dht_5.json -a 10.0.0.10 -p 7777 -T 5 -f 2 -i 1000 > pub1.out 2>&1 &
h10 python3 PublisherAppln.py -n pub4 -P 5 -S 5 -j dht_5.json -a 10.0.0.10 -p 7776 -T 6 -f 1 -i 1000 > pub4.out 2>&1 &
h13 python3 PublisherAppln.py -n pub2 -P 5 -S 5 -j dht_5.json -a 10.0.0.13 -p 7777 -T 5 -f 2 -i 2000 > pub2.out 2>&1 &
h2 python3 SubscriberAppln.py -n sub3 -P 5 -S 5 -j dht_5.json -T 6 > sub3.out 2>&1 &
h5 python3 SubscriberAppln.py -n sub4 -P 5 -S 5 -j dht_5.json -T 7 > sub4.out 2>&1 &
h14 python3 SubscriberAppln.py -n sub1 -P 5 -S 5 -j dht_5.json -T 5 > sub1.out 2>&1 &
h16 python3 SubscriberAppln.py -n sub5 -P 5 -S 5 -j dht_5.json -T 8 > sub5.out 2>&1 &
h18 python3 SubscriberAppln.py -n sub2 -P 5 -S 5 -j dht_5.json -T 7 > sub2.out 2>&1 &
