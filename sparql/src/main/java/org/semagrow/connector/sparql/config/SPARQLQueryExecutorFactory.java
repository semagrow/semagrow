package org.semagrow.connector.sparql.config;

import org.semagrow.evaluation.QueryExecutor;
import org.semagrow.evaluation.QueryExecutorConfigException;
import org.semagrow.evaluation.QueryExecutorFactory;
import org.semagrow.evaluation.QueryExecutorImplConfig;
import org.semagrow.connector.sparql.execution.SPARQLQueryExecutor;

/**
 * Created by angel on 6/4/2016.
 */
public class SPARQLQueryExecutorFactory implements QueryExecutorFactory {

    @Override
    public String getType() { return SPARQLQueryExecutorConfig.TYPE; }

    @Override
    public QueryExecutorImplConfig getConfig() { return new SPARQLQueryExecutorConfig(); }

    @Override
    public QueryExecutor getQueryExecutor(QueryExecutorImplConfig config)
            throws QueryExecutorConfigException
    {
        return new SPARQLQueryExecutor(null, null);
    }
}
