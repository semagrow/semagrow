package eu.semagrow.core.impl.sparql;

import eu.semagrow.core.eval.QueryExecutor;
import eu.semagrow.core.eval.QueryExecutorConfigException;
import eu.semagrow.core.eval.QueryExecutorFactory;
import eu.semagrow.core.eval.QueryExecutorImplConfig;

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
