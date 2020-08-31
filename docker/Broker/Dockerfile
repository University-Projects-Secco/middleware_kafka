# Alpine Linux with OpenJDK JRE
FROM openjdk:8-jre-alpine
# copy WAR into image
COPY kafka ./kafka
COPY server.properties ./server.properties
# install bash
RUN apk add --no-cache bash
# run application with this command line
CMD ["/bin/bash", "./kafka/bin/kafka-server-start.sh", "./server.properties"]
EXPOSE 9092