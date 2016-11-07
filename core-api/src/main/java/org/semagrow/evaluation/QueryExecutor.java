package org.semagrow.evaluation;

import org.semagrow.selector.Site;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.reactivestreams.Publisher;

import java.util.List;

/**
 * @author acharal
 */
public interface QueryExecutor {

    Publisher<BindingSet> evaluate(Site endpoint, TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException;

    Publisher<BindingSet> evaluate(Site endpoint, TupleExpr expr, List<BindingSet> bindings)
            throws QueryEvaluationException;

}
