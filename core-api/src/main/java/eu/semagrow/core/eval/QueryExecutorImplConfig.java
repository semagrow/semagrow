package eu.semagrow.core.eval;

import org.openrdf.model.Graph;
import org.openrdf.model.Resource;

/**
 * Created by angel on 30/3/2016.
 */
public interface QueryExecutorImplConfig {

    String getType();

    void validate() throws QueryExecutorConfigException;

    Resource export(Graph graph);

    void parse(Graph graph, Resource resource) throws QueryExecutorConfigException;

}
