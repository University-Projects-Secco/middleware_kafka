#!/bin/bash


#DEFAULT VALUES
NUM_BROKERS=2
KAFKA_PATH='.'
FUNCTIONS=()
REPLICAS=()
REPLICATION_FACTOR=2
VERBOSE=false


#PRINT OPTION EXPLANATION
function help() {
  echo "Valid options are: "
  echo "-z zookeeper address"
  echo "-f comma-separated functions executed on each stage, in stage order"
  echo "-r number of clients running for each stage"
  echo "[-k path to bin folder of kafka. Default is './bin']"
  echo "[-b desired number of brokers. Default is 2. The value will automatically be raised to the maximum number of replicas for one stage if needed]"
  echo "[-v enable verbose output]"
}

function error() {
    printf "$1\n">&2
    printf "\n"
    help
    exit 1
}

function check_int() {
  if ! [[ "$1" =~ ^[0-9]+$ ]]; then
    error "$2 must be an integer"
  fi
}

function split_string() {
  local -n res=$1
  IFS=',' # (,) is set as delimiter
  read -ra res <<< "$2" # FUNCTIONS_STRING is splitted into an array using IFS as delimiter
  IFS=' ' # reset to default IFS after usage
}

function check_variable() {
    local -n var=$1
    if ! [[ $var && ${var-x} ]]; then #Missing will be initialized if some of the above variables is unset
    error "missing required option -$2"
    echo missing required option: -"$2">&2
    help
    exit 1
fi
}

function check_brokers() {
  [[ $(bash "$KAFKA_PATH/bin/zookeeper-shell.sh" "$ZOOKEEPER" 'ls' '/brokers/ids') =~ \[(.+)\] ]]
  IFS=', ' # (,) is set as delimiter
  read -ra IDS <<< "${BASH_REMATCH[0]}" # FUNCTIONS_STRING is splitted into an array using IFS as delimiter
  IFS=' ' # reset to default IFS after usage
  if [ "$IDS" -ge $NUM_BROKERS ]; then
    print_verbose "Using $NUM_BROKERS out of $IDS actually running"
  else
    error "$NUM_BROKERS brokers running but $IDS are needed"
  fi
}

function print_verbose() {
  if [ "$VERBOSE" == "true" ]; then
    printf "%s\n" "$1"
  fi
}

function print_verbose_array() {
  if [ "$VERBOSE" == "true" ]; then
    local -n arr=$2
    printf "%s" "$1"
    for elem in "${arr[@]}"; do printf " %s" "$elem"; done
    printf "\n"
  fi
}

#PARSE INPUT OPTIONS
while getopts :z:f:r:k:R:b:v option
do
case "${option}" in
z) ZOOKEEPER=${OPTARG};;
f) FUNCTIONS_STRING=${OPTARG};;
r) REPLICAS_STRING=${OPTARG};;
k) KAFKA_PATH=${OPTARG};;
R) REPLICATION_FACTOR=${OPTARG}
  check_int "$REPLICATION_FACTOR" 'Replication factor'
  [ "$REPLICATION_FACTOR" -ge 2 ] || error "A replication factor <2 is unsafe" >&2
  ;;
b) NUM_BROKERS=${OPTARG}
    check_int "$NUM_BROKERS" 'Number of brokers'
    ;;
v) VERBOSE=true;;
:) error "Provide an argument for option ${option}";;
\?) error "invalid option: ${option}->${OPTARG}. Command options are:"
esac
done


#CHECK IF ALL VARIABLES ARE SET
check_variable ZOOKEEPER 'z'
check_variable FUNCTIONS_STRING 'f'
check_variable REPLICAS_STRING 'r'

#STRING SPLITTING
split_string FUNCTIONS "$FUNCTIONS_STRING"
split_string REPLICAS "$REPLICAS_STRING"
print_verbose_array 'Replicas:' REPLICAS
print_verbose_array 'Functions:' FUNCTIONS

#CHECK LENGTH, SAVE IT
if [ "${#REPLICAS[@]}" == ${#FUNCTIONS[@]} ]; then  #Check length of functions array, exit if invalid (!=num stages)
  NUM_STAGES="${#REPLICAS[@]}"
else
    error "invalid argument length: length of functions must equal the number of stages"
fi

if [ "${#REPLICAS[@]}" == ${#FUNCTIONS[@]} ]; then  #Check length of functions array, exit if invalid (!=num stages)
  NUM_STAGES="${#REPLICAS[@]}"
else
  error "invalid argument length: length of functions must equal the number of stages."
fi
print_verbose "NUMBER OF STAGES: $NUM_STAGES"


#CALCULATE REQUIRED BROKERS
i=0
until [ $i -ge "$NUM_STAGES" ]; do #increase numBrokers to the maximum value present in REPLICAS (max # of partitions) if it's actually lower
  (( NUM_BROKERS < i )) && NUM_BROKERS=i  #assignment will happen only if the first condition is met
  ((i++))
done



print_verbose "MINIMUM NUMBER OF BROKERS: $NUM_BROKERS"


#END OF INPUT ELABORATION
print_verbose ""


#CREATE "SYSTEM" TOPICS
if OUT=$(bash "$KAFKA_PATH/bin/kafka-topics.sh" --create --zookeeper "$ZOOKEEPER" --replication-factor "$REPLICATION_FACTOR" --partitions 1 --topic states \
  --config "cleanup.policy=compact" \
  --config "delete.retention.ms=100" \
  --config "segment.ms=100" \
  --config "min.cleanable.dirty.ratio=0.01" 2>&1); then
  print_verbose "$OUT"
  print_verbose "Created topic 'states'"
else
  error "Failed to create topic 'states'. Error output is:\n\n$OUT"
fi


#CREATE "APPLICATIVE" TOPICS
stage=0
until [ $stage -gt "$NUM_STAGES" ]; do
  if OUT=$(bash "$KAFKA_PATH/bin/kafka-topics.sh" --create --zookeeper "$ZOOKEEPER" --replication-factor "$REPLICATION_FACTOR" --partitions "${REPLICAS[((stage+1))]}" \
                --topic "topic_$stage" 2>&1); then
    print_verbose "$OUT"
    print_verbose Created topic "'topic_$stage'"
  else
    error "Failed to create topic 'topic_$stage'. Error output is:\n\t$OUT"
  fi
  ((stage++))
done