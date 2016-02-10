package eu.semagrow.core.impl.estimator;

import eu.semagrow.core.impl.planner.Cost;
import eu.semagrow.core.impl.planner.Site;
import org.openrdf.query.algebra.TupleExpr;

/**
 * The interface of the cost estimator
 * @author Angelos Charalambidis
 */
public interface CostEstimator {

    /**
     * Computes the cost of the execution plan {@code expr}
     * @param expr the execution plan
     * @return an estimated cost
     */
    Cost getCost(TupleExpr expr);

    /**
     * Computes the cost of the execution plan @{code expr} if that plan is executed in the site @{code source}
     * @param expr the execution plan
     * @param source the source to execute the plan
     * @return an estimated cost of execution in site @{link source}
     */
    Cost getCost(TupleExpr expr, Site source);

}
