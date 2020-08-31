KAFKA_PATH='/home/matteo/University/Middleware_technologies_for_distributed_systems/kafka_2.12-2.3.1'
NUM_BROKERS=2
REPLICATION_FACTOR=2
BOOTSTRAP_SERVERS=''
function help() {
    printf "check your input\n"
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

for (( i = 1; i <= NUM_BROKERS; i++ )); do
    BOOTSTRAP_SERVERS+="broker-$i"
done

docker network create kafka-net --driver bridge
bash run_zookeeper.sh "$KAFKA_PATH" '../Zookeeper'
bash run_brokers.sh "$NUM_BROKERS" "$KAFKA_PATH"
bash create_topics.sh -z 'zookeeper' -f "$FUNCTIONS_STRING" -r "$REPLICAS_STRING" \
      -k "$KAFKA_PATH" -R "$REPLICATION_FACTOR" -B "$BOOTSTRAP_SERVERS" "${VERBOSE+-v}" "${MACHINES+-m $MACHINES}"
bash run_clients.sh