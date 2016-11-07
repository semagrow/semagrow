package org.semagrow.estimator;

import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.semagrow.plan.Cost;

/**
 * The interface of the cost estimator
 * @author acharal
 */
public interface CostEstimator {

    /**
     * Computes the cost of a given tree of physical operators
     *
     * @param expr a tree of physical operators
     * @return the estimated cost
     */
    Cost getCost(TupleExpr expr);

}