package org.semagrow.geospatial.execution.config;

import org.semagrow.evaluation.QueryExecutor;
import org.semagrow.evaluation.QueryExecutorConfigException;
import org.semagrow.evaluation.QueryExecutorFactory;
import org.semagrow.evaluation.QueryExecutorImplConfig;
import org.semagrow.geospatial.execution.GeoSPARQLQueryExecutor;

public class GeoSPARQLQueryExecutorFactory implements QueryExecutorFactory {
    @Override
    public String getType() {
        return GeoSPARQLQueryExecutorConfig.TYPE;
    }

    @Override
    public QueryExecutorImplConfig getConfig() {
        return new GeoSPARQLQueryExecutorConfig();
    }

    @Override
    public QueryExecutor getQueryExecutor(QueryExecutorImplConfig config) throws QueryExecutorConfigException {
        return new GeoSPARQLQueryExecutor(null, null);
    }
}
