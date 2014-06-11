package eu.semagrow.stack.modules.api.evaluation;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 6/6/14.
 */
public interface QueryExecutor {

    CloseableIteration<BindingSet,QueryEvaluationException>
        evaluate(URI endpoint, TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException;

    CloseableIteration<BindingSet,QueryEvaluationException>
        evaluate(URI endpoint, TupleExpr expr, CloseableIteration<BindingSet,QueryEvaluationException> bindingIter)
            throws QueryEvaluationException;

}
