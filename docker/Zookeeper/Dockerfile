# Alpine Linux with OpenJDK JRE
FROM openjdk:8-jre-alpine
# copy WAR into image
COPY kafka ./kafka
# install bash
RUN apk add --no-cache bash
# run application with this command line
CMD ["/bin/bash", "./kafka/bin/zookeeper-server-start.sh", "./kafka/config/zookeeper.properties"]
EXPOSE 2181 2888 3888