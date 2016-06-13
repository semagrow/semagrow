package eu.semagrow.core.impl.evaluation;

import eu.semagrow.core.eval.*;
import eu.semagrow.core.source.Site;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.util.Optional;

/**
 * Created by antonis on 21/3/2016.
 */
public class StaticQueryExecutorResolver implements QueryExecutorResolver {

    ValueFactory vf = SimpleValueFactory.getInstance();

    QueryExecutor defaultExecutor;

    public StaticQueryExecutorResolver() {}

    public StaticQueryExecutorResolver(QueryExecutor defaultExecutor) {
        this.defaultExecutor = defaultExecutor;
    }

    @Override
    public QueryExecutor resolve(Site endpoint) {

        Optional<QueryExecutorFactory> factory = QueryExecutorRegistry.getInstance().get(endpoint.getType());

        if (factory.isPresent())
            try {
                return factory.get().getQueryExecutor(null);
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
