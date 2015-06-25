package eu.semagrow.core.evaluation;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 6/6/14.
 */
public interface EvaluationStrategy extends org.openrdf.query.algebra.evaluation.EvaluationStrategy {

    CloseableIteration<BindingSet,QueryEvaluationException>
        evaluate(TupleExpr tupleExpr, CloseableIteration<BindingSet, QueryEvaluationException> bIter)
            throws QueryEvaluationException;

}
