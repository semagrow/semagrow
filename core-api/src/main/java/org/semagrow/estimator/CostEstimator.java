package org.semagrow.estimator;

import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.semagrow.plan.Cost;

/**
 * The interface of the cost estimator
 * @author Angelos Charalambidis
 */
public interface CostEstimator {

    /**
     * Computes the cost of the execution plan {@code expr}
     *
     * @param expr the execution plan
     * @return an estimated cost
     */
    Cost getCost(TupleExpr expr);

}