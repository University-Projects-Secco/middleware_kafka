KAFKA_PATH=${1-'/home/matteo/University/Middleware_technologies_for_distributed_systems/kafka_2.12-2.3.1'}
PROPERTIES=${2-"$KAFKA_PATH/config/server.properties"}
bash "$KAFKA_PATH/bin/kafka-server-start.sh" "$PROPERTIES"