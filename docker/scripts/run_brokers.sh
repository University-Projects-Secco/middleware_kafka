NUMBER=${1-2}
KAFKA_PATH=${2-'/home/matteo/University/Middleware_technologies_for_distributed_systems/kafka_2.12-2.3.1'}
DIRECTORY="../Broker"

cp -r "$KAFKA_PATH" "$DIRECTORY/kafka"

for (( i = 1; i <= NUMBER; i++ )); do
  cp "$DIRECTORY/server_template.properties" "$DIRECTORY/server.properties"
  printf 'broker.id=%d' "$i">>"$DIRECTORY/server.properties"
  docker build -t "kafka/broker$i" "$DIRECTORY"
  docker run -d --rm --network 'kafka-net' --name "broker$i" "kafka/broker$i"
done

rm -R "$DIRECTORY/kafka" &
rm "$DIRECTORY/server.properties" &
