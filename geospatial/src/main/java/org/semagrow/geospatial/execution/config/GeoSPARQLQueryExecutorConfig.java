package org.semagrow.geospatial.execution.config;

import org.semagrow.connector.sparql.config.SPARQLQueryExecutorConfig;

public class GeoSPARQLQueryExecutorConfig extends SPARQLQueryExecutorConfig {

    public static String TYPE = "GeoSPARQL";

    @Override
    public String getType() { return TYPE; }
}
