package org.semagrow.evaluation.file;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;

/**
 * Created by angel on 10/20/14.
 */
public interface MaterializationManager {

    CloseableIteration<BindingSet,QueryEvaluationException>
        getResult(IRI handle) throws QueryEvaluationException;

    MaterializationHandle saveResult() throws QueryEvaluationException;

}
