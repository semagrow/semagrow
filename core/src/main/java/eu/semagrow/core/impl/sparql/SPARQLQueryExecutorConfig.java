package eu.semagrow.core.impl.sparql;

import eu.semagrow.core.eval.QueryExecutorConfigException;
import eu.semagrow.core.eval.QueryExecutorImplConfig;
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;

/**
 * Created by angel on 6/4/2016.
 */
public class SPARQLQueryExecutorConfig implements QueryExecutorImplConfig {

    public static String TYPE = "SPARQL";


    @Override
    public String getType() { return TYPE; }

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
