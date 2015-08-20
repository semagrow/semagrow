# Semagrow Stack


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

To build a fully functional tomcat with Semagrow preinstalled
issue the following command:

        mvn clean package -Psemagrow-stack-webapp-distribution

This will create the file:

     ${semagrow.root}/http/target/semagrow-http-${semagrow-version}-distribution.zip

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

SemaGrow uses UNION instead of VALUES to implement the BindJoin operator. This fails in 4store 1.1.5 
and previous versions in the presence of FILTER clauses due to unsafe optimization by 4store.
