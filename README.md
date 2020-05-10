# SF Crime Statistics with Spark Streaming

## Introduction 

The aim of the project is to create an Streaming application with Spark that connects to a Kafka cluster, reads and process the data.

## Requirements

* Java 1.8.x
* Scala 2.11.x
* Spark 2.4.x
* Kafka
* Python 3.6 or above

## How to use the application

In order to run the application you will need to start:

1. Zookeeper:

`/usr/bin/zookeeper-server-start config/zookeeper.properties`

2. Kafka server:

`/usr/bin/kafka-server-start config/server.properties`

3. Insert data into topic:

`python kafka_server.py`

4. Kafka consumer:

`kafka-console-consumer --topic "topic-name" --from-beginning --bootstrap-server localhost:9092`

5. Run Spark job:

`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py`

# Performance
On the workspace and using default configuration every job required 202 task and 4 secs to complete. by default spark repartition data between 200 partions, after changing `spark.sql.shuffle.partitions` to 1 the total number of tasks was reduced to 3 with less than 100 ms execution time. 
