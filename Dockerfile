FROM maven:3.3.3-jdk-8 as build

LABEL maintainer="Giannis Mouchakis <gmouchakis@iit.demokritos.gr>"

WORKDIR /semagrow

COPY . /semagrow

RUN mvn clean package


FROM tomcat:8.5-jre8-alpine

RUN mkdir -p /etc/default/semagrow

COPY --from=build /semagrow/assembly/src/main/resources/metadata.ttl /etc/default/semagrow/
COPY --from=build /semagrow/assembly/src/main/resources/repository.ttl /etc/default/semagrow

COPY --from=build /semagrow/webgui/target/SemaGrow.war $CATALINA_HOME/webapps/SemaGrow.war

EXPOSE 8080

CMD [ "catalina.sh", "run" ] 
