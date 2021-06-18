#syntax=docker/dockerfile:experimental

FROM maven:3.6-jdk-8 as build

LABEL maintainer="Giannis Mouchakis <gmouchakis@iit.demokritos.gr>"

WORKDIR /semagrow

COPY . /semagrow

RUN --mount=type=cache,target=/root/.m2 mvn clean package -P tomcat-bundle


FROM tomcat:8.5-jre8-slim

ENV SEMAGROW_HOME /opt/semagrow
ENV CATALINA_HOME $SEMAGROW_HOME
ENV PATH $CATALINA_HOME/bin:$PATH

RUN mkdir -p /etc/default/semagrow

COPY --from=build /semagrow/assembly/src/main/resources/metadata.ttl /etc/default/semagrow/
COPY --from=build /semagrow/assembly/src/main/resources/repository.ttl /etc/default/semagrow

COPY --from=build semagrow/assembly/target/semagrow-*-tomcat-bundle.tar.gz semagrow-tomcat.tar.gz

RUN mkdir -p $SEMAGROW_HOME \
 && tar zxvf semagrow-tomcat.tar.gz -C $SEMAGROW_HOME \
 && rm semagrow-tomcat.tar.gz

WORKDIR $SEMAGROW_HOME

EXPOSE 8080

CMD [ "catalina.sh", "run" ] 
