package eu.semagrow.stack.modules.sails.semagrow.evaluation.iterator;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.query.BindingSet;

/**
 * Provision for heterogeneous responses that must be transformed
 */
public class QueryTransformationIteration implements CloseableIteration<BindingSet,Exception> {

    public void close() throws Exception {

    }

    public boolean hasNext() throws Exception {
        return false;
    }

    public BindingSet next() throws Exception {
        return null;
    }

    public void remove() throws Exception {

    }
}
