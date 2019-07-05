----- STEPS TO TEST PRODUCER -----------
1) Start zookeeper server from below command:

./zkServer.sh start-foreground


2) Create topic as given below :

./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic weather


3)List topics created as given below:

./kafka-topics.sh --list --zookeeper localhost:2181


4) Start kafka server as given below:

./kafka-server-start.sh ../config/server.properties


5) Run kafka producer created as below:

scala -classpath producers-0.1.jar weather.analyzer.producers.ReproducibleWeatherProducer SERVERS


6) Run kafka console consumer as given below to check data:

./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weather --from-beginning --property print.key=true
