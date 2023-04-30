h1 /usr/share/zookeeper/bin/zkServer.sh start-foreground > zk_log.txt 2>&1 &

h1 python3 BrokerAppln.py -n broker1 -a 10.0.0.1 -p 6666 -g group1 -z 10.0.0.1:2181 > log_broker1.out 2>&1 &
h2 python3 BrokerAppln.py -n broker2 -a 10.0.0.2 -p 6666 -g group2 -z 10.0.0.1:2181 > log_broker2.out 2>&1 &
h3 python3 BrokerAppln.py -n broker3 -a 10.0.0.3 -p 6666 -g group3 -z 10.0.0.1:2181 > log_broker3.out 2>&1 &

h4 python3 DiscoveryAppln.py -n disc1 -s 5556 -a 10.0.0.4 -p 5555 -z 10.0.0.1:2181 > log_disc1.out 2>&1 &
h5 python3 DiscoveryAppln.py -n disc2 -s 5556 -a 10.0.0.5 -p 5555 -z 10.0.0.1:2181 > log_disc2.out 2>&1 &





h4 python3 PublisherAppln.py -n pub13 -P 20 -S 10 -j dht.json -a 10.0.0.4 -p 7777 -T 6 -f 2 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub13.out 2>&1 &
h6 python3 PublisherAppln.py -n pub5 -P 20 -S 10 -j dht.json -a 10.0.0.6 -p 7777 -T 6 -f 0.75 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub5.out 2>&1 &
h6 python3 PublisherAppln.py -n pub19 -P 20 -S 10 -j dht.json -a 10.0.0.6 -p 7776 -T 6 -f 4 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub19.out 2>&1 &
h7 python3 PublisherAppln.py -n pub8 -P 20 -S 10 -j dht.json -a 10.0.0.7 -p 7777 -T 5 -f 3 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub8.out 2>&1 &
h9 python3 PublisherAppln.py -n pub2 -P 20 -S 10 -j dht.json -a 10.0.0.9 -p 7777 -T 9 -f 1 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub2.out 2>&1 &
h9 python3 PublisherAppln.py -n pub10 -P 20 -S 10 -j dht.json -a 10.0.0.9 -p 7776 -T 7 -f 2 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub10.out 2>&1 &
h11 python3 PublisherAppln.py -n pub7 -P 20 -S 10 -j dht.json -a 10.0.0.11 -p 7777 -T 9 -f 3 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub7.out 2>&1 &
h11 python3 PublisherAppln.py -n pub11 -P 20 -S 10 -j dht.json -a 10.0.0.11 -p 7776 -T 7 -f 3 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub11.out 2>&1 &
h12 python3 PublisherAppln.py -n pub6 -P 20 -S 10 -j dht.json -a 10.0.0.12 -p 7777 -T 9 -f 4 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub6.out 2>&1 &
h12 python3 PublisherAppln.py -n pub9 -P 20 -S 10 -j dht.json -a 10.0.0.12 -p 7776 -T 8 -f 3 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub9.out 2>&1 &
h12 python3 PublisherAppln.py -n pub12 -P 20 -S 10 -j dht.json -a 10.0.0.12 -p 7775 -T 5 -f 2 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub12.out 2>&1 &
h13 python3 PublisherAppln.py -n pub20 -P 20 -S 10 -j dht.json -a 10.0.0.13 -p 7777 -T 5 -f 3 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub20.out 2>&1 &
h14 python3 PublisherAppln.py -n pub17 -P 20 -S 10 -j dht.json -a 10.0.0.14 -p 7777 -T 5 -f 0.5 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub17.out 2>&1 &
h15 python3 PublisherAppln.py -n pub16 -P 20 -S 10 -j dht.json -a 10.0.0.15 -p 7777 -T 9 -f 0.75 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub16.out 2>&1 &
h16 python3 PublisherAppln.py -n pub1 -P 20 -S 10 -j dht.json -a 10.0.0.16 -p 7777 -T 6 -f 0.25 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub1.out 2>&1 &
h16 python3 PublisherAppln.py -n pub15 -P 20 -S 10 -j dht.json -a 10.0.0.16 -p 7776 -T 8 -f 0.75 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub15.out 2>&1 &
h18 python3 PublisherAppln.py -n pub18 -P 20 -S 10 -j dht.json -a 10.0.0.18 -p 7777 -T 5 -f 0.25 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub18.out 2>&1 &
h19 python3 PublisherAppln.py -n pub4 -P 20 -S 10 -j dht.json -a 10.0.0.19 -p 7777 -T 7 -f 0.75 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub4.out 2>&1 &
h19 python3 PublisherAppln.py -n pub14 -P 20 -S 10 -j dht.json -a 10.0.0.19 -p 7776 -T 6 -f 3 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub14.out 2>&1 &
h20 python3 PublisherAppln.py -n pub3 -P 20 -S 10 -j dht.json -a 10.0.0.20 -p 7777 -T 6 -f 1 -i 25 -z 10.0.0.1:2181 -en 3BROKERS > log_pub3.out 2>&1 &
h1 python3 SubscriberAppln.py -n sub8 -P 20 -S 10 -j dht.json -T 5 -z 10.0.0.1:2181 > log_sub8.out 2>&1 &
h6 python3 SubscriberAppln.py -n sub7 -P 20 -S 10 -j dht.json -T 7 -z 10.0.0.1:2181 > log_sub7.out 2>&1 &
h7 python3 SubscriberAppln.py -n sub6 -P 20 -S 10 -j dht.json -T 7 -z 10.0.0.1:2181 > log_sub6.out 2>&1 &
h8 python3 SubscriberAppln.py -n sub5 -P 20 -S 10 -j dht.json -T 9 -z 10.0.0.1:2181 > log_sub5.out 2>&1 &
h9 python3 SubscriberAppln.py -n sub1 -P 20 -S 10 -j dht.json -T 5 -z 10.0.0.1:2181 > log_sub1.out 2>&1 &
h11 python3 SubscriberAppln.py -n sub10 -P 20 -S 10 -j dht.json -T 6 -z 10.0.0.1:2181 > log_sub10.out 2>&1 &
h13 python3 SubscriberAppln.py -n sub3 -P 20 -S 10 -j dht.json -T 5 -z 10.0.0.1:2181 > log_sub3.out 2>&1 &
h15 python3 SubscriberAppln.py -n sub9 -P 20 -S 10 -j dht.json -T 9 -z 10.0.0.1:2181 > log_sub9.out 2>&1 &
h17 python3 SubscriberAppln.py -n sub4 -P 20 -S 10 -j dht.json -T 7 -z 10.0.0.1:2181 > log_sub4.out 2>&1 &
h20 python3 SubscriberAppln.py -n sub2 -P 20 -S 10 -j dht.json -T 9 -z 10.0.0.1:2181 > log_sub2.out 2>&1 &
