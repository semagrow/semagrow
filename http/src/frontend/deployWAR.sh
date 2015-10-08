#!/bin/bash
clear
cd $1/http/src/frontend
gulp
echo "\n\033[4;31m---------- Gulp ends here! ----------\n\033[0m"
cd ../../target/bin
sh catalina.sh stop
cd ../../../
mvn package
echo "\n\033[4;32m---------- Maven ends here! ----------\n\033[0m"
cd http/target
rm -rf domains/localhost/webapps/*
cp SemaGrow.war domains/localhost/webapps/
sh bin/catalina.sh start

