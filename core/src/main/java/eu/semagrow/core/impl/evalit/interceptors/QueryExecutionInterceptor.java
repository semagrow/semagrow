package eu.semagrow.core.impl.evalit.interceptors;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * Created by angel on 6/27/14.
 */
public interface QueryExecutionInterceptor extends EvaluationSessionAwareInterceptor {

    CloseableIteration<BindingSet,QueryEvaluationException>
        afterExecution(IRI endpoint, TupleExpr expr, BindingSet bindings,
                    CloseableIteration<BindingSet,QueryEvaluationException> result);

    CloseableIteration<BindingSet,QueryEvaluationException>
        afterExecution(IRI endpoint, TupleExpr expr, CloseableIteration<BindingSet,QueryEvaluationException> bindings,
                    CloseableIteration<BindingSet,QueryEvaluationException> result);
}
