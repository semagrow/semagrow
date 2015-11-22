package eu.semagrow.core.evaluation;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;


/**
 * Evaluation Strategy
 * 
 * @author Angelos Charalambidis
 */

public interface EvaluationStrategy extends org.openrdf.query.algebra.evaluation.EvaluationStrategy
{

    CloseableIteration<BindingSet,QueryEvaluationException>
    evaluate( TupleExpr tupleExpr, CloseableIteration<BindingSet, QueryEvaluationException> bIter )
    		throws QueryEvaluationException;

}
