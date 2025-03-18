## docker command

### access docker
docker exec -it kafka-1 sh -c "cd /opt/kafka/bin && /bin/bash"

### kafka create topic
 ./kafka-topics.sh --bootstrap-server localhost:19092 --create --topic my_topic

### kafka delete topic
 ./kafka-topics.sh --bootstrap-server localhost:19092 --delete --topic my_topic

### kafka show topic
 ./kafka-topics.sh --bootstrap-server localhost:19092 --list

### kafka consumer topic
 ./kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic my_topic

