KAFKA_PATH=${1-'/home/matteo/University/Middleware_technologies_for_distributed_systems/kafka_2.12-2.3.1/'}
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DIRECTORY="$SCRIPT_DIR/../Zookeeper/"
cp -R "$KAFKA_PATH" "$DIRECTORY/kafka/"
docker build -t "kafka/zookeeper" "$DIRECTORY"
docker run -d --name 'zookeeper' --network 'kafka-net' --ip 'zookeeper' "kafka/zookeeper"
rm -r "$DIRECTORY/kafka"