# Semagrow
[![GitHub license](https://img.shields.io/badge/license-Apache%202-blue.svg)](https://raw.githubusercontent.com/semagrow/semagrow/main/LICENSE)
[![Build Status](https://travis-ci.org/semagrow/semagrow.svg?branch=main)](https://travis-ci.org/semagrow/semagrow)

Semagrow is a federated SPARQL query processor that allows combining, cross-indexing and, in general,
making the best out of all public data, regardless of their size, update rate, and schema.

Semagrow offers a single SPARQL endpoint that serves data from remote data sources and that hides
from client applications heterogeneity in both form (federating non-SPARQL endpoints) and
meaning (transparently mapping queries and query results between vocabularies).

The main difference between Semagrow and most existing distributed querying solutions is
that Semagrow targets the federation of heterogeneous and independently provided data sources.

In other words, Semagrow aims to offer the most efficient distributed querying solution that
can be achieved without controlling the way data is distributed between sources and,
in general, without having the responsibility to centrally manage the data sources of the
federation.

## Getting Started

### Building

Building Semagrow from sources requires to have a system with JDK8 and Maven 3.1 or higher.  
Optionally, you may need a PostgreSQL as a requirement for the query transformation
functionality.

To build Semagrow you should type:
```bash
$ mvn clean install
```
in the top-level project directory. This will result in jar file
in the target directory of the respective module and in a war file in the target directory of
the `webgui` module that can be deployed to the Servlet server of your choice.

#### Bundled with Apache Tomcat

Moreover, Semagrow can be build pre-bundled with the Apache Tomcat servlet server.
To achieve that you could issue
```bash
$ mvn clean package -P tomcat-bundle
```
from the top-level directory of the project. This will result in a
compressed file in the target directory of the `assembly` module 
containing a fully equipped Apache Tomcat with Semagrow pre-installed.
However, please note that external dependencies such as the
PostgresSQL database needs to be installed and run separately.

#### Building a Docker image from sources

You can also test your build deployed in a docker image (Docker 18.09 or newer required for building). To do so run at the project root directory:
```bash
$ DOCKER_BUILDKIT=1 docker build -t semagrow .
```
The produced image will be tagged as `semagrow:latest` and will contain Tomcat with Semagrow deployed.

### Configuration

By default, Semagrow look for its configuration files in `/etc/default/semagrow`
and expects to find at least a `repository.ttl` and a `metadata.ttl` file in
order to establish a federation of endpoints. The `repository.ttl` describes
the configuration of the Semagrow endpoint, while the `metadata.ttl` describes
the endpoints to be federated. The `repository.ttl` configuration file
also defines the location of the `metadata.ttl` that can be changed to the desired
path.

Samples of these configuration files can be found as
[resources of the `http` module](https://github.com/semagrow/semagrow/tree/main/http/src/main/resources)

### Running Semagrow

#### Running Semagrow from the Apache Tomcat bundle

In order to run the bundle of Apache Tomcat with SemaGrow you should

1. uncompress the generated zip,
2. copy the files from the `resources` folder to `/etc/default/semagrow` and
3. run the `startup.sh` script located in the `bin` folder.

SemaGrow can be accessed at `http://localhost:8080/SemaGrow/`.

#### Running Semagrow using Docker

Semagrow has an [official docker repository](https://github.com/semagrow/docker-semagrow)
and official docker images are available in [Docker Hub](https://hub.docker.com/r/semagrow/semagrow/).

To run semagrow using the latest official docker image you should execute
```bash
$ docker run -d semagrow/semagrow
```
Howeover, you can also build your own docker image using the steps described in Section [Building](#### Building a Docker image from sources)
The produced image will be tagged as `semagrow` and will contain Tomcat with Semagrow deployed.

To run the newly produced image you should execute
```bash
$ docker run -d semagrow
```
or if you want to test Semagrow with your configuration files (`repository.ttl` and `metadata.ttl`) issue
```bash
$ docker run -d -v /path/to/configuration:/etc/default/semagrow semagrow
```

In either case you can access Semagrow at `http://<CONTAINER_IP>:8080/SemaGrow/`
where `<CONTAINER_IP>` is the address assigned to the semagrow container and can
be retrieved using [`docker inspect`](https://docs.docker.com/engine/reference/commandline/inspect/)

## Known issues

* SemaGrow uses UNION instead of VALUES to implement the BindJoin operator. This fails in 4store 1.1.5 and previous versions in the  presence of FILTER clauses due to an unsafe optimization by 4store.
* When deploying in Glassfish 4 by coping the SemaGrow.war file in the autodeploy directory, Semagrow is accessible at `http://DOMAIN/SemaGrow/index.jsp` instead of `http://DOMAIN/SemaGrow/`

