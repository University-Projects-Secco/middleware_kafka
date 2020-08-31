KAFKA_PATH=${1-'/home/matteo/University/Middleware_technologies_for_distributed_systems/kafka_2.12-2.3.1'}
PROPERTIES=${2-"$KAFKA_PATH/config/server.properties"}
DIRECTORY="../Broker"

while true; do
  printf "How many brokers?\n"
  # shellcheck disable=SC2162
  read NUMBER
  case $NUMBER in
  ^[0-9]+$ ) break ;;
  *) printf "Insert an integer number\n" ;;
  esac
done

cp -r "$KAFKA_PATH" "$DIRECTORY/kafka"
cp "$PROPERTIES" "$DIRECTORY/server.properties"

for (( i = 1; i <= NUMBER; i++ )); do
  docker build -t "kafka/broker$i" "$DIRECTORY"
  docker run --ip "172.0.0.$i" --name "broker$i" --rm "kafka/broker$i"
done

rm -r "$DIRECTORY/kafka"
