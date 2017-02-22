package org.semagrow.evaluation.file;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.QueryResultHandler;

import java.io.IOException;

/**
 * A QueryResultHandler that is used as materialization point
 * The results are passed to the handler using the handleSolution
 * and the results are commited/saved using the endQueryResults().
 * To discard the handle use destroy.
 * Created by angel on 10/20/14.
 */
public interface MaterializationHandle extends QueryResultHandler {

    IRI getId();

    void destroy() throws IOException;
}
