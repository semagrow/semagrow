package eu.semagrow.core.impl.estimator;

import eu.semagrow.core.impl.planner.Cost;
import eu.semagrow.core.impl.planner.Site;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 4/25/14.
 */
public interface CostEstimator {

    Cost getCost(TupleExpr expr);

    Cost getCost(TupleExpr expr, Site source);

}
