# Semagrow Stack
[![Build Status](https://travis-ci.org/semagrow/semagrow.svg?branch=main)](https://travis-ci.org/semagrow/semagrow)

## System Requirements

The following external dependencies must be satisfied:

1. git >= 1.8.1.4
2. java >= 1.8
3. maven >= 3.0.3

The following external dependencies are not required, but provide
extra functionality if satisfied:

1. PostgreSQL is needed for the query transformation functionality


## Building Semagrow from sources

### Building jar/war distribution

To build semagrow simply cd into ${semagrow-stack-modules.root} and
issue the following command:

    mvn clean install

This will result in a jar file in the target directory of the
respective module and also deploy the jars to the local maven
repository (~/.m2/repository on linux).


### Building bundled with Apache Tomcat

To build a fully functional tomcat with the semagrow preinstalled cd into ${semagrow.root} and issue the following command 

        mvn clean package -Psemagrow-stack-webapp-distribution

This will result in a zip file in ${semagrow.root}/http/target
containing a fully equipped tomcat (in version 7.0.42 as of writing).
This tomcat is configured with all dependencies (lib, JNDI) that are
needed to run the semagrow. Please note that external dependencies
need to be setup individually. For example a the Postgres database
needs to be installed and run separately. In order to run SemaGrow
uncompress the generated zip, copy the files from the "resources"
folder to /etc/default/semagrow and run the .startup.sh script located
in the "bin" folder. If you do not have permissions to create
directories under /etc/default then copy the files from the
"resources" folder to /tmp and in the resources/repository.ttl file
edit line

This file contains a fully equipped Tomcat 7.0.42. This Tomcat is
configured with all dependencies (lib, JNDI) that are needed to run
the semagrow.


## Configuration

In order to run SemaGrow uncompress the generated zip,
copy the files from the "resources" folder to /etc/default/semagrow
and run the .startup.sh script located in the "bin" folder. If you do
not have permissions to create directories under /etc/default/ then
copy the files from the "resources" folder to any directory and edit
accordingly the following line in the file resources/repository.ttl :

    semagrow:metadataInit "/etc/default/semagrow/metadata.ttl" ;

Now you can run the startup.sh script.
SemaGrow can be accessed at http://localhost:8080/SemaGrow/ .


##KNOWN ISSUES

SemaGrow uses UNION instead of VALUES to implement the BindJoin
operator. This fails in 4store 1.1.5 and previous versions in the
presence of FILTER clauses due to an unsafe optimization by 4store.
