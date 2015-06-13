package eu.semagrow.stack.modules.sails.semagrow.estimator;

import eu.semagrow.stack.modules.sails.semagrow.planner.Cost;
import eu.semagrow.stack.modules.sails.semagrow.planner.Site;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 4/25/14.
 */
public interface CostEstimator {

    Cost getCost(TupleExpr expr);

    Cost getCost(TupleExpr expr, Site source);

}
