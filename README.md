# analytics-20

## Commands

### Start zookeeper
./zookeeper-server-start.bat ./z.properties
### Start broker
./kafka-console-producer.bat --broker-list localhost:9092 --topic homework

### Create topic homework & homework-result
./kafka-topics.bat --create --topic homework --partitions 1 --zookeeper localhost:2181 --replication-factor 1
./kafka-topics.bat --create --topic homework-result --partitions 1 --zookeeper localhost:2181 --replication-factor 1

### Delete topic
./kafka-topics.bat --zookeeper localhost:2181 --delete --topic homework
./kafka-topics.bat --zookeeper localhost:2181 --delete --topic homework-result

### Fill topic with input data
cat kafka-messages.jsonline | ./kafka-console-producer.bat --broker-list localhost:9092 --topic homework

### Read topic in console
./kafka-console-consumer.bat --topic homework-result --from-beginning --bootstrap-server localhost:9092