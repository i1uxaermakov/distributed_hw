h1 /usr/share/zookeeper/bin/zkServer.sh start-foreground > zk_log.txt 2>&1 &

h1 python3 BrokerAppln.py -n broker1 -a 10.0.0.1 -p 6666 -g group1 -z 10.0.0.1:2181 > log_broker1.out 2>&1 &
h2 python3 BrokerAppln.py -n broker2 -a 10.0.0.2 -p 6666 -g group2 -z 10.0.0.1:2181 > log_broker2.out 2>&1 &
h3 python3 BrokerAppln.py -n broker3 -a 10.0.0.3 -p 6666 -g group3 -z 10.0.0.1:2181 > log_broker3.out 2>&1 &

h4 python3 DiscoveryAppln.py -n disc1 -s 5556 -a 10.0.0.4 -p 5555 -z 10.0.0.1:2181 > log_disc1.out 2>&1 &
h5 python3 DiscoveryAppln.py -n disc2 -s 5556 -a 10.0.0.5 -p 5555 -z 10.0.0.1:2181 > log_disc2.out 2>&1 &




