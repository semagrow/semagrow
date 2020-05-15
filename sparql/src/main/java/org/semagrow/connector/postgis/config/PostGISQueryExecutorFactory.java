package org.semagrow.connector.postgis.config;

import org.semagrow.evaluation.QueryExecutor;
import org.semagrow.evaluation.QueryExecutorConfigException;
import org.semagrow.evaluation.QueryExecutorFactory;
import org.semagrow.evaluation.QueryExecutorImplConfig;
import org.semagrow.connector.postgis.execution.PostGISQueryExecutor;

public class PostGISQueryExecutorFactory implements QueryExecutorFactory {
	
	@Override
    public String getType() { 
		return PostGISQueryExecutorConfig.TYPE; 
	}

    @Override
    public QueryExecutorImplConfig getConfig() { 
    	return new PostGISQueryExecutorConfig(); 
    }

    @Override
    public QueryExecutor getQueryExecutor(QueryExecutorImplConfig config) throws QueryExecutorConfigException {
        return new PostGISQueryExecutor(null, null);
    }
}
