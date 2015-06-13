package eu.semagrow.stack.modules.sails.semagrow.evaluation.file;

import org.openrdf.model.URI;
import org.openrdf.query.QueryResultHandler;

import java.io.IOException;

/**
 * A QueryResultHandler that is used as materialization point
 * The results are passed to the handler using the handleSolution
 * and the results are commited/saved using the endQueryResults().
 * To discard the handle use destroy.
 * Created by angel on 10/20/14.
 */
public interface MaterializationHandle extends QueryResultHandler {

    public URI getId();

    public void destroy() throws IOException;
}
