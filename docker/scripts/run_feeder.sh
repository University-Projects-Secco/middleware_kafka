#!/bin/bash
printf "bootstrap.servers=%s" "$1">../Feeder/config.properties
docker build -t "kafka/feeder" "../Feeder"
docker run --rm -i -t --network 'kafka-net' --name "feeder" "kafka/feeder"