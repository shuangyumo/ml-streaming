### Overview
A simple machine learning stream application on Kafka and Flink: consumes messages from an input topic, treats them 
 with a ML algorithm (for the moment XGboost) and publishes the result on an output topic.

### Useful documentation
* Flink: https://ci.apache.org/projects/flink/flink-docs-release-1.6/quickstart/scala_api_quickstart.html
* Kafka Connector: https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html

### Build
The project template was created with ```bash <(curl https://flink.apache.org/q/sbt-quickstart.sh)```

An assembly JAR is built and sftp'ed to a testing server (see publish section of ```built.sbt```)

In sbt shell run: ```;clean; assembly; publish ```

### Testing

Install https://github.com/hbraux/dku which provides a set of light Docker images for Kafka, Flink, etc.

On target host, start Nginx, Flink and Kafka containers, and create input and output topics:
```sh
dku nginx
dku flink
dku kafka
dku kafka kafka-topics.sh --zookeeper kafka:2181 --create --topic in --partitions 1 --replication-factor 1
dku kafka kafka-topics.sh --zookeeper kafka:2181 --create --topic out --partitions 1 --replication-factor 1
```

Copy the model file to container and submit the predict application (127 is the number of features in the samples)
Publish some input data in svmlib format on Kafka input topic
and check the output topic
```sh
docker cp agaricus.model flink:/data/agaricus.model
dku submit flink test-scala-kafka --brokers kafka:9092 --in in --out out --model /data/agaricus.model 127

dku kafka kafka-console-producer.sh --broker-list kafka:9092 --topic in
0 1:1 9:1 19:1 21:1 24:1 34:1 36:1 39:1 42:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 117:1 122:1
1 3:1 9:1 19:1 21:1 30:1 34:1 36:1 40:1 41:1 53:1 58:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 118:1 124:1
(CTRL-D)

dku kafka  kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic out --from-beginning


```

** Trainging (not working yet) **


```sh
curl -O https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.test
curl -O https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.train

docker cp agaricus.txt.train flink:/data/agaricus.train
dku submit flink test-scala-kafka -c test.scala.kafka.XGBoostTrain /data/agaricus.train
```