package eu.semagrow.core.impl.evaluation;

import eu.semagrow.core.eval.*;
import eu.semagrow.core.source.Site;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Created by antonis on 21/3/2016.
 */
public class StaticQueryExecutorResolver implements QueryExecutorResolver {

    ValueFactory vf = ValueFactoryImpl.getInstance();

    QueryExecutor defaultExecutor;

    public StaticQueryExecutorResolver() {}

    public StaticQueryExecutorResolver(QueryExecutor defaultExecutor) {
        this.defaultExecutor = defaultExecutor;
    }

    @Override
    public QueryExecutor resolve(Site endpoint) {

        QueryExecutorFactory factory = QueryExecutorRegistry.getInstance().get(endpoint.getType());

        if (factory != null)
            try {
                return factory.getQueryExecutor(null);
            } catch (QueryExecutorConfigException e) {
                return this.defaultExecutor;
            }

        return this.defaultExecutor;

        //FIXME
        /*
        if (endpoint.getURI().equals(vf.createURI("http://my.cassandra.antru/"))) {
            CassandraQueryExecutorImpl cassandraExecutor = new CassandraQueryExecutorImpl();
            cassandraExecutor.setCredentials("127.0.0.1", 9042, "myDatabase");
            return cassandraExecutor;
        }
        else {
            return this.defaultExecutor;
        }
        */

    }
}
