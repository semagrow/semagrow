package eu.semagrow.stack.modules.sails.semagrow.planner;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 27/4/2015.
 */
public interface PlanOptimizer {

    Plan getBestPlan(TupleExpr expr, BindingSet bindings, Dataset dataset);

}
