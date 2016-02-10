package eu.semagrow.core.impl.evaluation.interceptors;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 6/27/14.
 */
public interface QueryEvaluationInterceptor extends EvaluationSessionAwareInterceptor {

    CloseableIteration<BindingSet,QueryEvaluationException>
        afterEvaluation(TupleExpr expr, BindingSet bindings,
                 CloseableIteration<BindingSet,QueryEvaluationException> result);

    CloseableIteration<BindingSet,QueryEvaluationException>
        afterEvaluation(TupleExpr expr, CloseableIteration<BindingSet,QueryEvaluationException> bindings,
                    CloseableIteration<BindingSet,QueryEvaluationException> result);
}
