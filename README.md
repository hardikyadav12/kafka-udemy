# kafka-udemy

Trying to learn how kafka works!

Steps To Connect To Kafka:

1. zookeeper-server-start.sh ~/kafka_2.12-3.7.0/config/zookeeper.properties

2. kafka-server-start.sh ~/kafka_2.12-3.7.0/config/server.properties

3. kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --create --partitions 3 --replication-factor 1

Steps 1,2,3 help us to start kafka server and create a topic with name as "first_topic"

Below are some useful commands to check topic details and start the consumer and see the consumed messages: 

1. kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic topic1 --partitions 5

2. kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic topic1 --partitions 5

3. kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first_topic

4. kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --from-beginning

5. kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --from-beginning
