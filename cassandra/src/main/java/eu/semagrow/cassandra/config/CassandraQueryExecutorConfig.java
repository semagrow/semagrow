package eu.semagrow.cassandra.config;

import eu.semagrow.core.eval.QueryExecutorConfigException;
import eu.semagrow.core.eval.QueryExecutorImplConfig;
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;

/**
 * Created by angel on 5/4/2016.
 */
public class CassandraQueryExecutorConfig implements QueryExecutorImplConfig {

    public static String TYPE = "CASSANDRA";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void validate() throws QueryExecutorConfigException {

    }

    @Override
    public Resource export(Graph graph) {
        return null;
    }

    @Override
    public void parse(Graph graph, Resource resource) throws QueryExecutorConfigException {

    }

}
