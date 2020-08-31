#!/bin/bash

for properties in ./*.properties; do
  if [ -f "$properties" ]; then
    fileName=$(echo "$properties" | cut -f 2 -d '/')
    machineName=$(echo "$fileName" | cut -f 1 -d '.')
    machineNum=$(echo "$machineName" | cut -f 2 -d '-')
    directory="../$machineName"
    rm -r "$directory"
    cp -r "../Pipeline" "$directory"
    cp "$properties" "$directory/config.properties"
    docker build -t "kafka/$machineName" "$directory"
    docker run -d --network 'kafka-net' --ip "client-$machineNum" --name "$machineName" "kafka/$machineName"
  fi;
done