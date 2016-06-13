package eu.semagrow.core.estimator;

import eu.semagrow.core.plan.Cost;
import eu.semagrow.core.source.Site;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

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
