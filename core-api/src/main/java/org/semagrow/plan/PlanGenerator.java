package org.semagrow.plan;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * The interface that describes the possible methods to construct
 * plans out of expressions and combine plans to more complex ones.
 * @author acharal
 */
public interface PlanGenerator {

    /**
     * Constructs all the possible plans by a simple abstract expressions, and possible
     * bindings and dataset
     * @param expr the abstract expression
     * @param bindings possible non-empty binding set that refer to variables in {@code expr}
     * @param dataset possible non-empty referring datasets
     * @return a possibly empty collection of valid execution plans
     */
    PlanCollection accessPlans(TupleExpr expr, BindingSet bindings, Dataset dataset);

    /**
     * Constructs all the possible plans by combining (i.e. joining) simpler plans in all possible (and valid) ways
     * @param p1 a collection of simpler plans
     * @param p2 a collection of simpler plans
     * @return a collection of more complex plans that occur using a plan
     * from {@code p1} and a plan from {@code p2} collections
     */
    PlanCollection joinPlans(PlanCollection p1, PlanCollection p2);

    /**
     * Constructs enhanced plans that satisfy the {@code desiredProperties}
     * @param plans a collection of plans that might not satisfy all the {@code desiredProperties}
     * @param desiredProperties a collection of plan properties that must be satisfied
     * @return a collection of plans that contain the given plans altered in such a way to satisfy
     * the desired properties.
     */
    PlanCollection finalizePlans(PlanCollection plans, PlanProperties desiredProperties);

}
