#!/bin/bash

for properties in ./*.properties; do
  if [ -f "$properties" ]; then
    fileName=$(echo "$properties" | cut -f 2 -d '/')
    machineName=$(echo "$fileName" | cut -f 1 -d '.')
    directory="../$machineName"
    cp -R "../Pipeline" "$directory"
    mv "$properties" "$directory/config.properties"
    docker build -t "kafka/$machineName" "$directory"
    docker run -d --rm --network 'kafka-net' --name "$machineName" "kafka/$machineName"
    rm -R "$directory" &
  fi;
done