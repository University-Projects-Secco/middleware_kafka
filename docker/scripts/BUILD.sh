KAFKA_PATH='/home/matteo/University/Middleware_technologies_for_distributed_systems/kafka_2.12-2.3.1'
NUM_BROKERS=2
REPLICATION_FACTOR=2
BOOTSTRAP_SERVERS=''
function help() {
  printf "Check your input. Command options are:\n"
  printf "-f comma-separated functions executed on each stage, in stage order. Don't put spaces\n"
  printf "-r comma-separated number of clients running for each stage. Don't put spaces\n"
  printf "[-m preferred number of machines]\n"
  printf "[-R topic replication factor. Higher valuer will grant higher fault tolerance. Minimum value allowed is 2. Must be integer]\n"
  printf "[-b desired number of brokers. Default is 2. The value will automatically be raised to the maximum number of replicas for one stage if needed]\n"
  printf "[-B comma separated list of available bootstrap servers. Don't put spaces. If not specified, they will have to be added manually to the generated properties]\n"
  printf "[-v enable verbose output]"
}
function error() {
  printf "$1\n" >&2
  printf "\n"
  help
  exit 1
}
function check_variable() {
  local -n var=$1
  if ! [[ $var && ${var-x} ]]; then #Missing will be initialized if some of the above variables is unset
    error "missing required option -$2"
  fi
}
function check_int() {
  if ! [[ "$1" =~ ^[0-9]+$ ]]; then
    error "$2 must be an integer"
  fi
}
while getopts :k:f:r:m:R:b:B:v option; do
  case "${option}" in
  k) KAFKA_PATH=${OPTARG} ;;
  f) FUNCTIONS_STRING=${OPTARG} ;;
  r) REPLICAS_STRING=${OPTARG} ;;
  m) MACHINES=${OPTARG} ;;
  R)
    REPLICATION_FACTOR=${OPTARG}
    check_int "$REPLICATION_FACTOR" 'Replication factor'
    [ "$REPLICATION_FACTOR" -ge 2 ] || error "A replication factor <2 is unsafe" >&2
    ;;
  b)
    NUM_BROKERS=${OPTARG}
    check_int "$NUM_BROKERS" 'Number of brokers'
    ;;
  B) BOOTSTRAP_SERVERS=${OPTARG} ;;
  v) VERBOSE=true ;;
  :) error "Provide an argument for option ${option}" ;;
  \?) error "invalid option: ${option}->${OPTARG}. Command options are:" ;;
  esac
done

#CHECK IF ALL VARIABLES ARE SET
check_variable FUNCTIONS_STRING 'f'
check_variable REPLICAS_STRING 'r'

BOOTSTRAP_SERVERS+="broker1:9092"
LISTENERS='PLAINTEXT://broker1:9092,SSL://broker1:9092'
for (( i = 2; i <= NUM_BROKERS; i++ )); do
    BOOTSTRAP_SERVERS+=",broker$i:9092"
    LISTENERS+=",PLAINTEXT://broker$i:9092,SSL://broker$i:9092"
done

docker network create kafka-net --driver bridge
bash run_zookeeper.sh "$KAFKA_PATH" '../Zookeeper'
bash run_brokers.sh "$NUM_BROKERS" "$KAFKA_PATH" "$LISTENERS"

echo
echo
echo '---------------------'
echo 'kafka infrastructure deployed'
echo '---------------------'
echo
echo

sleep 5s

bash create_topics.sh -z 'localhost:2181' -f "$FUNCTIONS_STRING" -r "$REPLICAS_STRING" \
      -k "$KAFKA_PATH" -R "$REPLICATION_FACTOR" -B "$BOOTSTRAP_SERVERS" "${VERBOSE+-v}" "${MACHINES+-m $MACHINES}"

echo
echo
echo '---------------------'
echo 'created topics'
echo '---------------------'
echo
echo

bash run_clients.sh

echo
echo
echo '---------------------'
echo 'pipeline deployed'
echo '---------------------'
echo
echo

bash run_feeder.sh "$BOOTSTRAP_SERVERS"