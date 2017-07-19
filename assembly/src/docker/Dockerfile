FROM tomcat:8.0.37-jre8-alpine

MAINTAINER Yiannis Mouchakis <gmouchakis@iit.demokritos.gr>

RUN mkdir -p /etc/default/semagrow

COPY repository.ttl /etc/default/semagrow/
COPY metadata.ttl /etc/default/semagrow/

COPY semagrow-assembly-*.war $CATALINA_HOME/webapps/SemaGrow.war

COPY logback.groovy $CATALINA_HOME/libs

CMD catalina.sh run
