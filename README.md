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

Building Semagrow from sources requires to have a system with JDK8 and maven 3. 
Optionally, you may need a PostgreSQL as a requirement for the query transformation 
functionality.

To build Semagrow you should type:
```bash
$ mvn clean install
```
in the top-level project directory. This will result in jar file 
in the target directory of the respective module and in a war file for
the `http` module that can be deployed to the Servlet server of your choice.

#### Bundled with Apache Tomcat

Moreover, Semagrow can be build pre-bundled with the Apache Tomcat servlet server.
To achieve that you could issue
```bash
$ mvn clean package -Psemagrow-stack-webapp-distribution
```
from the top-leve directory of the project. This will result in a 
zip file in `./http/target` containing a fully equipped Apache Tomcat 
with pre-installed Semagrow.
However, please note that external dependencies such as the 
PostgresSQL database needs to be installed and run separately. 

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

In order to run the bundle of Apache Tomcat with SemaGrow you should

1. uncompress the generated zip, 
2. copy the files from the `resources` folder to `/etc/default/semagrow` and 
3. run the `startup.sh` script located in the `bin` folder. 

SemaGrow can be accessed at `http://localhost:8080/SemaGrow/`.


## Known issues

* SemaGrow uses UNION instead of VALUES to implement the BindJoin operator. This fails in 4store 1.1.5 and previous versions in the  presence of FILTER clauses due to an unsafe optimization by 4store.
