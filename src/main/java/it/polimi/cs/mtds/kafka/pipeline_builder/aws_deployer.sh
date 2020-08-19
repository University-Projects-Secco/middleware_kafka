#!/bin/bash
AWS_CLUSTER_CONFIG="./awsclusterconfig.json"
AWS_MSK_ARN=""
AWS_CLUSTER_STATE=""
NUM_BROKERS=1
FUNCTIONS=()
REPLICAS=()

#parse input options
while getopts N:C:b:n:f:r:mB: option
do
case "${option}" in
N) AWS_CLUSTER_NAME=${OPTARG};;
C) AWS_CLUSTER_CONFIG=${OPTARG};;
b) BOOTSTRAP_SERVERS=${OPTARG};;
n) NUM_STAGES=${OPTARG};;
f) FUNCTIONS_STRING=${OPTARG};;
r) REPLICAS_STRING=${OPTARG};;
m) MONITORING_LEVEL=${OPTARG};;
B) NUM_BROKERS=${OPTARG};;
*) echo "invalid option: ${option}->${OPTARG}. Command options are:"
  echo "-N name of aws cluster"
  echo "-b bootstrap_servers"
  echo "-n number of stages in the pipeline"
  echo "-f comma-separated functions executed on each stage, in stage order"
  echo "-r number of processes running for each process"
  echo "[-C path to AWS MSK cluster configuration file]"
  echo "[-m monitoring level for the MSK cluster] "
  echo "[-B desired number of brokers. Default is 1. The value will automatically be raised to the maximum number of replicas for one stage if needed]"
esac
done

#Split options into array using (,) as a delimiter
IFS=',' # (,) is set as delimiter
read -ra FUNCTIONS <<< "$FUNCTIONS_STRING" # FUNCTIONS_STRING is read into an FUNCTIONS as tokens separated by IFS
read -ra REPLICAS <<< "$REPLICAS_STRING"
IFS=' ' # reset to default value after usage

#Check length of functions array
if [ "$NUM_STAGES" != ${#FUNCTIONS[@]} ]; then
    printf "invalid argument length: length of functions must equal the number of stages. Functions: "
    for i in "${FUNCTIONS[@]}"; do printf "%s " "$i"; done
    echo ". Expected length: $NUM_STAGES"
    exit 1
#Check length of replicas array
elif [ "$NUM_STAGES" != ${#REPLICAS[@]} ]; then
    printf "invalid argument length: length of replicas must equal the number of stages. Replicas: "
    for i in "${REPLICAS[@]}"; do printf "%s " "$i"; done
    echo ". Expected length: $NUM_STAGES"
    exit 1
fi

#increase numBrokers to the maximum value present in REPLICAS (max # of partitions) if it's actually lower
for i in "${REPLICAS[@]}"; do
  (( NUM_BROKERS < i )) && NUM_BROKERS=i  #assignment will happen only if condition is met
done

#TODO: create client subnets
#TODO: create security groups

#create MSK cluster and lunch brokers and store result on file
aws kafka create-cluster --cluster-name "$AWS_CLUSTER_NAME" \
--broker-node-group-info "$AWS_CLUSTER_CONFIG" \
--kafka-version "2.3.1" \
--number-of-broker-nodes "$NUM_BROKERS" \
--enhanced-monitoring "$MONITORING_LEVEL" > "clusterCreationOutput.json"

#initialize ARN and STATE from the file saved after cluster creation
mapfile AWS_MSK_ARN="$(jq .ClusterArn clusterCreationOutput)" #save ARN
mapfile AWS_CLUSTER_STATE"$(jq .State clusterCreationOutput)" #save initial cluster state

#wait for cluster to be created or creation to fail
while [ "$AWS_CLUSTER_STATE" = "CREATING" ]; do
  sleep 5s
  AWS_CLUSTER_STATE="$(jq .State "$(aws kafka describe-cluster --cluster-arn "$AWS_MSK_ARN")")"
done

#if creation failed, stop
if [ "$AWS_CLUSTER_STATE" = "FAILED" ]; then
    echo "Failed to create cluster"
    exit 1
fi

#TODO: create EC2 instances

#TODO: create messaging topics with the correct number of partitions (MAX of replicas)

#create topic for state management TODO: change it in AWS style
./bin/kafka-topics.sh --create --zookeeper "$BOOTSTRAP_SERVERS" --replication-factor 2 --partitions 1 --topic states \
--config "cleanup.policy=compact" \
--config "delete.retention.ms=100" \
--config "segment.ms=100" \
--config "min.cleanable.dirty.ratio=0.01"

#TODO: run jar on EC2 machines