h7 python3 DiscoveryAppln.py -n disc2 -j dht5_ent15.json -p 5555 -P 5 -S 10 > disc2.out 2>&1 &
h12 python3 DiscoveryAppln.py -n disc1 -j dht5_ent15.json -p 5555 -P 5 -S 10 > disc1.out 2>&1 &
h13 python3 DiscoveryAppln.py -n disc3 -j dht5_ent15.json -p 5555 -P 5 -S 10 > disc3.out 2>&1 &
h14 python3 DiscoveryAppln.py -n disc4 -j dht5_ent15.json -p 5555 -P 5 -S 10 > disc4.out 2>&1 &
h15 python3 DiscoveryAppln.py -n disc5 -j dht5_ent15.json -p 5555 -P 5 -S 10 > disc5.out 2>&1 &
h1 python3 PublisherAppln.py -n pub3 -P 5 -S 10 -j dht5_ent15.json -a 10.0.0.1 -p 7777 -T 5 -f 0.75 -i 2000 > pub3.out 2>&1 &
h4 python3 PublisherAppln.py -n pub1 -P 5 -S 10 -j dht5_ent15.json -a 10.0.0.4 -p 7777 -T 9 -f 4 -i 3000 > pub1.out 2>&1 &
h4 python3 PublisherAppln.py -n pub2 -P 5 -S 10 -j dht5_ent15.json -a 10.0.0.4 -p 7776 -T 5 -f 1 -i 1000 > pub2.out 2>&1 &
h5 python3 PublisherAppln.py -n pub5 -P 5 -S 10 -j dht5_ent15.json -a 10.0.0.5 -p 7777 -T 9 -f 4 -i 2000 > pub5.out 2>&1 &
h12 python3 PublisherAppln.py -n pub4 -P 5 -S 10 -j dht5_ent15.json -a 10.0.0.12 -p 7777 -T 7 -f 0.5 -i 2000 > pub4.out 2>&1 &
h1 python3 SubscriberAppln.py -n sub2 -P 5 -S 10 -j dht5_ent15.json -T 6 > sub2.out 2>&1 &
h3 python3 SubscriberAppln.py -n sub3 -P 5 -S 10 -j dht5_ent15.json -T 6 > sub3.out 2>&1 &
h3 python3 SubscriberAppln.py -n sub5 -P 5 -S 10 -j dht5_ent15.json -T 7 > sub5.out 2>&1 &
h5 python3 SubscriberAppln.py -n sub7 -P 5 -S 10 -j dht5_ent15.json -T 9 > sub7.out 2>&1 &
h5 python3 SubscriberAppln.py -n sub10 -P 5 -S 10 -j dht5_ent15.json -T 5 > sub10.out 2>&1 &
h9 python3 SubscriberAppln.py -n sub8 -P 5 -S 10 -j dht5_ent15.json -T 5 > sub8.out 2>&1 &
h12 python3 SubscriberAppln.py -n sub4 -P 5 -S 10 -j dht5_ent15.json -T 9 > sub4.out 2>&1 &
h13 python3 SubscriberAppln.py -n sub6 -P 5 -S 10 -j dht5_ent15.json -T 8 > sub6.out 2>&1 &
h14 python3 SubscriberAppln.py -n sub9 -P 5 -S 10 -j dht5_ent15.json -T 5 > sub9.out 2>&1 &
h19 python3 SubscriberAppln.py -n sub1 -P 5 -S 10 -j dht5_ent15.json -T 8 > sub1.out 2>&1 &
