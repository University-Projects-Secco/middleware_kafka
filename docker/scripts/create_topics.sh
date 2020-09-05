#!/bin/bash

#DEFAULT VALUES
NUM_BROKERS=2
KAFKA_PATH='.'
FUNCTIONS=()
REPLICAS=()
REPLICATION_FACTOR=2
VERBOSE=false
MACHINES=0
BOOTSTRAP_SERVERS='<Replace with actual addresses>'

#PRINT OPTION EXPLANATION
function help() {
  echo "Valid options are:\n"
  echo "-z zookeeper address\n"
  echo "-f comma-separated functions executed on each stage, in stage order. Don't put spaces\n"
  echo "-r comma-separated number of clients running for each stage. Don't put spaces\n"
  echo "[-m preferred number of machines]\n"
  echo "[-k path to bin folder of kafka. Default is './bin']\n"
  echo "[-R topic replication factor. Higher valuer will grant higher fault tolerance. Minimum value allowed is 2. Must be integer]\n"
  echo "[-B comma separated list of available bootstrap servers. Don't put spaces. If not specified, they will have to be added manually to the generated properties]\n"
  echo "[-b desired number of brokers. Default is 2. The value will automatically be raised to the maximum number of replicas for one stage if needed]\n"
  echo "[-v enable verbose output]"
}

function error() {
  #bash ./clear.sh &>/dev/null #clear kafka topics. for development utility
  # shellcheck disable=SC2059
  printf "$1\n" >&2
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
  IFS=${3-','}              # (,) is set as delimiter
  # shellcheck disable=SC2034
  read -ra res <<<"$2" # FUNCTIONS_STRING is splitted into an array using IFS as delimiter
  IFS=' '              # reset to default IFS after usage
}

function check_variable() {
  local -n var=$1
  if ! [[ $var && ${var-x} ]]; then #Missing will be initialized if some of the above variables is unset
    error "missing required option -$2"
  fi
}

function check_brokers() {
  [[ $(bash "$KAFKA_PATH/bin/zookeeper-shell.sh" "$ZOOKEEPER" 'ls' '/brokers/ids') =~ \[(.+)\] ]]
  split_string IDS "${BASH_REMATCH[1]}" ", "
  if [ "${#IDS[@]}" -ge $NUM_BROKERS ]; then
    print_verbose "Using $NUM_BROKERS out of ${#IDS[@]} actually running"
  else
    error "$NUM_BROKERS brokers needed but ${#IDS[@]} are running"
  fi
}

function check_machines() {
  if [ "$MACHINES" -gt 0 ] && [ "$MACHINES" -lt "$OPTIMAL_MACHINES" ]; then
    while true; do
      printf "A number of %d machines is suggested for this configuration.\n" "$OPTIMAL_MACHINES"
      printf "A number of %d machines has been provided." "$MACHINES"
      printf "Continue with the (p)rovided number of machines, with the (o)ptimal number, or (a)bort?\n"
      # shellcheck disable=SC2162
      read OPTION
      case $OPTION in
      [Pp]*) break ;;
      [Oo]*)
        MACHINES=$OPTIMAL_MACHINES
        break
        ;;
      [Aa]*) exit ;;
      *) printf "Please answer (p)rovided, (o)ptimal, (a)bort\n" ;;
      esac
    done
  elif [ "$MACHINES" -eq 0 ]; then
    MACHINES="$OPTIMAL_MACHINES"
  fi
}

function print_verbose() {
  if [ "$VERBOSE" == "true" ]; then
    printf "%s\n" "$1"
  fi
}

function create_properties() {
  machines=()
  remaining_replicas=("${REPLICAS[@]}")
  stage=0
  for (( i=0; i < "$OPTIMAL_MACHINES"; i++)); do
    machines[$((i % MACHINES))]+="$stage,"
    ((remaining_replicas[stage]--))
    if [[ "${remaining_replicas[$stage]}" == 0 ]]; then
      ((remaining_replicas[stage]=REPLICAS[stage]))
      ((stage++))
    fi
  done
  for (( i = 0; i < "${#machines[@]}"; i++ )); do
    stages=()
    split_string stages "${machines[$i]}"
    file="machine$i".properties
    printf "bootstrap.servers=%s\n" "$BOOTSTRAP_SERVERS">$file
    printf "stages=%s" $(("${stages[0]}"+1))>>$file
    for stage in "${stages[@]:1}"; do
      printf ",%s" $((stage+1))>>$file
    done
    printf "\nfunctions=%s" "${FUNCTIONS[${stages[0]}]}">>$file
    for stage in "${stages[@]:1}"; do
      printf ",%s" "${FUNCTIONS[$stage]}">>$file
    done
    printf "\nids=%d" "${remaining_replicas[${stages[0]}]}">>"$file"
    (("remaining_replicas[stages[0]]--"))
    #
    for stage in "${stages[@]:1}"; do
      printf ",%d" "${remaining_replicas[$stage]}">>"$file"
      (("remaining_replicas[stage]--"))
    done
  done
}

#PARSE INPUT OPTIONS
while getopts :z:f:r:m:k:R:b:B:v option; do
  case "${option}" in
  z) ZOOKEEPER=${OPTARG} ;;
  f) FUNCTIONS_STRING=${OPTARG} ;;
  r) REPLICAS_STRING=${OPTARG} ;;
  m) MACHINES=${OPTARG} ;;
  k) KAFKA_PATH=${OPTARG} ;;
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
check_variable ZOOKEEPER 'z'
check_variable FUNCTIONS_STRING 'f'
check_variable REPLICAS_STRING 'r'

#STRING SPLITTING
split_string FUNCTIONS "$FUNCTIONS_STRING"
split_string REPLICAS "$REPLICAS_STRING"
print_verbose "Replicas: ${REPLICAS[*]}"
print_verbose "Functions: ${FUNCTIONS[*]}"

#CHECK LENGTH, SAVE IT
if [ "${#REPLICAS[@]}" == ${#FUNCTIONS[@]} ]; then #Check length of functions array, exit if invalid (!=num stages)
  NUM_STAGES="${#REPLICAS[@]}"
else
  error "invalid argument length: length of functions must equal the number of stages"
fi

if [ "${#REPLICAS[@]}" == ${#FUNCTIONS[@]} ]; then #Check length of functions array, exit if invalid (!=num stages)
  NUM_STAGES="${#REPLICAS[@]}"
else
  error "invalid argument length: length of functions must equal the number of stages."
fi
print_verbose "Provided number of stages: $NUM_STAGES"

#CALCULATE REQUIRED BROKERS AND THE OPTIMAL NUMBER OF MACHINES (1 MACHINE PER REPLICA)
for (( i = 0; i < NUM_STAGES; i++ )); do #increase numBrokers to the maximum value present in REPLICAS (max # of partitions) if it's actually lower
  NUM_BROKERS=$((NUM_BROKERS < i ? i : NUM_BROKERS))
  ((OPTIMAL_MACHINES += REPLICAS[i]))
done

NUM_BROKERS=$((NUM_BROKERS > REPLICATION_FACTOR ? NUM_BROKERS : REPLICATION_FACTOR))
check_machines
print_verbose "Required number of brokers: $NUM_BROKERS"
check_brokers

#END OF INPUT ELABORATION
print_verbose ""

#CREATE PROPERTY FILES
create_properties

#CREATE "SYSTEM" TOPICS
if OUT=$(bash "$KAFKA_PATH/bin/kafka-topics.sh" --create --zookeeper "$ZOOKEEPER" --replication-factor "$REPLICATION_FACTOR" --partitions 1 --topic states \
  --config "cleanup.policy=compact" \
  --config "delete.retention.ms=100" \
  --config "segment.ms=100" \
  --config "min.cleanable.dirty.ratio=0.01" 2>&1); then
  print_verbose "$OUT"
else
  error "Failed to create topic 'states'. Error output is:\n\n$OUT"
fi

#CREATE "APPLICATIVE" TOPICS
for (( stage = 0; stage < NUM_STAGES; stage++ )); do
    if OUT=$(bash "$KAFKA_PATH/bin/kafka-topics.sh" --create --zookeeper "$ZOOKEEPER" --replication-factor "$REPLICATION_FACTOR" --partitions "${REPLICAS[$stage]}" \
    --topic "topic_$((stage+1))" 2>&1); then
    print_verbose "$OUT"
  else
    error "Failed to create topic 'topic_$stage'. Error output is:\n\t$OUT"
  fi
done