package org.semagrow.evaluation;

import org.eclipse.rdf4j.common.lang.service.ServiceRegistry;

/**
 * Created by angel on 30/3/2016.
 */
public class QueryExecutorRegistry extends ServiceRegistry<String, QueryExecutorFactory> {

    private static QueryExecutorRegistry registry;

    public static synchronized QueryExecutorRegistry getInstance() {
        if (registry == null)
            registry = new QueryExecutorRegistry();

        return registry;
    }

    public QueryExecutorRegistry() { super(QueryExecutorFactory.class); }

    @Override
    public String getKey(QueryExecutorFactory factory) { return factory.getType(); }

}
