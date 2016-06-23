package org.semagrow.estimator;

import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;


/**
 * Selectivity Estimator
 *
 * <p>Interface for any component that estimates the selectivity of
 * a given expression.</p>
 *
 * @author Angelos Charalambidis
 */
public interface SelectivityEstimator {

    /**
     * Estimate the merge selectivity factor {@code f} of a merge, such that
     * merge cardinality = cross product cardinality * {@code f}.
     * @param expr the merge expression
     * @return the selectivity factor
     */

    double getJoinSelectivity(Join expr);

    /**
     * Estimates the number of distinct values of a given variable.
     * @param varName the name of the variable
     * @param expr
     * @return the estimated number of distinct values of a variable.
     */

    double getVarSelectivity(String varName, TupleExpr expr);


    /**
     * Estimates the reduction factor {@code f} of applying {@code condition}
     * to expression {@code expr}. That is, the cardinality of the results is
     * cardinality of results of {@code expr} * {@code f}.
     * @param condition
     * @param expr
     * @return
     */

    double getConditionSelectivity(ValueExpr condition, TupleExpr expr);

}
