#!/bin/sh

cd kafka_2.11-2.0.0
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic wingyplus
