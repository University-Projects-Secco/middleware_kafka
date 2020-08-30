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
    docker run --ip "172.0.100.$machineNum" --name "$machineName" --rm "kafka/$machineName"
  fi;
done