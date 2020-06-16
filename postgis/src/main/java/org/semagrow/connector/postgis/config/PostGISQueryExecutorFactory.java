package org.semagrow.connector.postgis.config;

import org.semagrow.connector.postgis.execution.PostGISQueryExecutor;
import org.semagrow.evaluation.QueryExecutor;
import org.semagrow.evaluation.QueryExecutorFactory;
import org.semagrow.evaluation.QueryExecutorImplConfig;

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
    public QueryExecutor getQueryExecutor(QueryExecutorImplConfig config) {
//        return new PostGISQueryExecutor(null, null);
        return new PostGISQueryExecutor();
    }
    
}
