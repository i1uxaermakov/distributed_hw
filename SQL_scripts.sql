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