# Alpine Linux with OpenJDK JRE
FROM openjdk:8-jre-alpine
# copy WAR into image
COPY Feeder-1.0-SNAPSHOT.jar ./Feeder.jar
COPY libs ./libs
COPY config.properties ./config.properties
# run application with this command line
CMD ["/usr/bin/java", "-jar", "/Feeder.jar"]