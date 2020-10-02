# Playing around with Alpakka

## Running Kafka

I grabbed a Docker compose setup from confluent as follows

```
git clone https://github.com/confluentinc/cp-all-in-one
cd cp-all-in-one
git checkout 5.5.1-post
cd cp-all-in-one-community
docker-compose up -d
```

## Running my samples

The descriptively named Alpakka1 consumes a topic and commits as it goes, doing some phony business logic 

Alpakka2, which comes from the same school of amazing naming, produces sample data.

## What's going on with my consumer group?

Download the binary for Kafka (probably doesn't matter which Scala version)

https://www.apache.org/dyn/closer.cgi?path=/kafka/2.5.1/kafka_2.12-2.5.1.tgz
Extract it then directly run...

`./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group group2 --describe`

Then you can get the details as follows:

```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
group2          topic1          0          30000           30000           0  
```






