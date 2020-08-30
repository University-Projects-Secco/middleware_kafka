KAFKA_PATH=${1-'/home/matteo/University/Middleware_technologies_for_distributed_systems/kafka_2.12-2.3.1'}
DIRECTORY="../Zookeeper"
cp -r "$KAFKA_PATH" "$DIRECTORY/kafka"
docker build -t "kafka/zookeeper" "$DIRECTORY"
docker run --ip "172.0.0.0" --name "zookeeper" --rm "kafka/zookeeper"
rm -r "$DIRECTORY/kafka"