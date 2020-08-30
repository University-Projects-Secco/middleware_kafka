KAFKA_PATH=${1-'/home/matteo/University/Middleware_technologies_for_distributed_systems/kafka_2.12-2.3.1'}
bash "$KAFKA_PATH/bin/zookeeper-server-start.sh" "$KAFKA_PATH/config/zookeeper.properties"