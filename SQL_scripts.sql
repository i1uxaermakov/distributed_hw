create table latencies (
    entry_id int NOT NULL AUTO_INCREMENT,
    latency_sec DOUBLE(10,5),
    frequency int,
    num_topics int,
    pub_num int,
    sub_num int,
    dissemination VARCHAR(255),
    pub_id VARCHAR(255),
    sub_id VARCHAR(255),
    experiment_name VARCHAR(255),
    PRIMARY KEY (entry_id)
);

        --   "ISREADY",
        --   isready_latency,
        --   self.lookup, # lookup strategy
        --   self.pub_num, # num of publishers
        --   self.sub_num, # num of subscribers
        --   self.mw_obj.dht_num, # num of dht nodes
        --   self.name # id of publisher

create table dht_latencies (
    entry_id int NOT NULL AUTO_INCREMENT,
    type_of_request VARCHAR(255),
    latency_sec DOUBLE(10,5),
    lookup_strategy VARCHAR(255),
    pub_num int,
    sub_num int,
    dht_num int,
    entity_id VARCHAR(255),
    PRIMARY KEY (entry_id)
)

INSERT INTO dht_latencies(type_of_request, latency_sec, lookup_strategy, pub_num, sub_num, dht_num, entity_id) 
      VALUES (%s,%s,%s,%s,%s,%s,%s)


delete from latencies;

show tables;

desc latencies;


drop table latencies;

INSERT INTO latencies(latency_sec, frequency, num_topics, pub_num, sub_num, pub_id, sub_id, experiment_name) VALUES ();


select * from latencies;
select count(*) from latencies;

show databases;


SELECT experiment_name, AVG(latency_sec)
FROM latencies
GROUP BY experiment_name



select pub_num+sub_num as entity_num, dht_num, type_of_request, avg(latency_sec) as avg_latency
from dht_latencies
group by entity_num, dht_num, type_of_request
having type_of_request="ISREADY";