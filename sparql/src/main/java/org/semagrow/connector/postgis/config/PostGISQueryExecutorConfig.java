package org.semagrow.connector.postgis.config;

import org.semagrow.evaluation.QueryExecutorConfigException;
import org.semagrow.evaluation.QueryExecutorImplConfig;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

public class PostGISQueryExecutorConfig implements QueryExecutorImplConfig {

    public static String TYPE = "POSTGIS";

    @Override
    public String getType() { 
    	return TYPE; 
    }

    @Override
    public void validate() throws QueryExecutorConfigException {

    }

    @Override
    public Resource export(Model graph) {
        return null;
    }

    @Override
    public void parse(Model graph, Resource resource) throws QueryExecutorConfigException {

    }
}
