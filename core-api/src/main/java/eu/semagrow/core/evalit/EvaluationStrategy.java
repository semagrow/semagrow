package eu.semagrow.core.evalit;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;


/**
 * An extension of the EvaluationStrategy interface of OpenRDF that
 * supports an evaluation using a stream of bindings instead of a single BindingSet.
 * 
 * @author Angelos Charalambidis
 */

public interface EvaluationStrategy extends org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy
{

    /**
     * Evaluates a @{link TupleExpr} under an iteration of @{link BindingSet}s.
     * @param tupleExpr the expression to evaluate
     * @param bIter a closable iteration of @{link BindingSet}s to be used as different BindingSets
     *              on the variables of the {@code tupleExpr}
     * @return a closable iteration that contains  @{link BindingSet}s on the variables
     * of the {@code tupleExpr}
     * @throws QueryEvaluationException when the evaluation fails
     */
    CloseableIteration<BindingSet,QueryEvaluationException>
        evaluate( TupleExpr tupleExpr, CloseableIteration<BindingSet, QueryEvaluationException> bIter )
    		throws QueryEvaluationException;

}
