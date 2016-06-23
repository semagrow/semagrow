package org.semagrow.evaluation;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

/**
 * Created by angel on 30/3/2016.
 */
public interface QueryExecutorImplConfig {

    String getType();

    void validate() throws QueryExecutorConfigException;

    Resource export(Model graph);

    void parse(Model graph, Resource resource) throws QueryExecutorConfigException;

}
