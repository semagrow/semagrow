package eu.semagrow.core.impl.sparql;

import eu.semagrow.core.eval.QueryExecutorConfigException;
import eu.semagrow.core.eval.QueryExecutorImplConfig;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

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
    public Resource export(Model graph) {
        return null;
    }

    @Override
    public void parse(Model graph, Resource resource) throws QueryExecutorConfigException {

    }
}
