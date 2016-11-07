package org.semagrow.estimator;

import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * Cardinality Estimator
 *
 * <p>Interface for any component that estimates the number
 * of results expected when executing a given expression.</p>
 *
 * @author acharal
 */
public interface CardinalityEstimator {


    /**
     * This method estimates the cardinality of the results of executing
     * {@code expr} without making reference to a specific data source.
     * This method call is not valid for all types of expressions,
     * as some expressions can only be estimated in reference to a
     * specific data source.
     * @param expr a logical or physical tree of expressions
     * @return an estimated number of results
     */
    long getCardinality(TupleExpr expr);

}
