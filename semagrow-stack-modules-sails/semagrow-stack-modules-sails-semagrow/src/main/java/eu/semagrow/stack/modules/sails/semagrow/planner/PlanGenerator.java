package eu.semagrow.stack.modules.sails.semagrow.planner;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;

import java.util.Collection;

/**
 * Created by angel on 27/4/2015.
 */
public interface PlanGenerator {

    Collection<Plan> accessPlans(TupleExpr expr, BindingSet bindings, Dataset dataset);

    Collection<Plan> joinPlans(Collection<Plan> p1, Collection<Plan> p2);

    Collection<Plan> finalizePlans(Collection<Plan> plans, PlanProperties desiredProperties);

}
