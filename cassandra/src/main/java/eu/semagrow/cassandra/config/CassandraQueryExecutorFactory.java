package eu.semagrow.cassandra.config;

import eu.semagrow.cassandra.eval.CassandraQueryExecutorImpl;
import eu.semagrow.core.eval.QueryExecutor;
import eu.semagrow.core.eval.QueryExecutorConfigException;
import eu.semagrow.core.eval.QueryExecutorFactory;
import eu.semagrow.core.eval.QueryExecutorImplConfig;

/**
 * Created by angel on 5/4/2016.
 */
public class CassandraQueryExecutorFactory implements QueryExecutorFactory {

    @Override
    public String getType() {
        return CassandraQueryExecutorConfig.TYPE;
    }

    @Override
    public QueryExecutorImplConfig getConfig() {
        return new CassandraQueryExecutorConfig();
    }

    @Override
    public QueryExecutor getQueryExecutor(QueryExecutorImplConfig config) throws QueryExecutorConfigException {
        return new CassandraQueryExecutorImpl();
    }

}
