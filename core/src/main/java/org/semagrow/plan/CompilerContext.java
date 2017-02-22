package org.semagrow.plan;

import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.semagrow.estimator.CardinalityEstimatorResolver;
import org.semagrow.estimator.CostEstimatorResolver;
import org.semagrow.selector.SourceSelector;

import java.util.Collection;

/**
 * @author acharal
 */
public interface CompilerContext {

    CostEstimatorResolver getCostEstimatorResolver();

    CardinalityEstimatorResolver getCardinalityEstimatorResolver();

    SourceSelector getSourceSelector();

    /**
     *
     * @param physicalExpr
     * @return
     */
    Plan asPlan(TupleExpr physicalExpr);

    /**
     *
     * @param physicalExpr
     * @param props
     * @return
     */
    Plan asPlan(TupleExpr physicalExpr, PlanProperties props);

    /**
     *
     * @param physicalExpr
     * @param requestedProps
     * @return
     */
    Collection<Plan> enforceProps(Plan physicalExpr, RequestedPlanProperties requestedProps);

    /**
     *
     * @param plans
     * @param requestedProps
     * @return
     */
    Collection<Plan> enforceProps(Collection<Plan> plans, RequestedPlanProperties requestedProps);


    /**
     * Decides whether a {@link Plan} {@code p1} is suboptimal and should be safely pruned.
     * The decision may consult a collection of equivalent plans.
     * @param p1 the {@link Plan} which is questioned whether to be pruned or not.
     * @param plans the collection of equivalent plans to help deciding.
     * @return True if the plan in question should be pruned; false otherwise.
     */
    boolean canPrune(Plan p1, Collection<Plan> plans);

    void prune(Collection<Plan> plans);

}
