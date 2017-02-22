package org.semagrow.plan;

import org.eclipse.rdf4j.query.algebra.*;
import org.semagrow.estimator.CardinalityEstimatorResolver;
import org.semagrow.estimator.CostEstimatorResolver;
import org.semagrow.selector.Site;


/**
 * The default implementation of a {@link PlanFactory}
 * @author acharal
 * @since 2.0
 */
@Deprecated
public class SimplePlanFactory implements PlanFactory {

    private final CostEstimatorResolver costEstimatorResolver;

    private final CardinalityEstimatorResolver cardinalityEstimatorResolver;

    public SimplePlanFactory(CostEstimatorResolver costEstimatorResolver,
                             CardinalityEstimatorResolver cardinalityEstimatorResolver)
    {
        this.costEstimatorResolver = costEstimatorResolver;
        this.cardinalityEstimatorResolver = cardinalityEstimatorResolver;
    }


    public Plan create(TupleExpr expr) {
        return create(expr, PlanProperties.defaultProperties());
    }


    public Plan create(TupleExpr expr, PlanProperties initialProps)
    {
        PlanProperties props = PlanPropertiesUpdater.process(expr, initialProps);

        Site s = props.getSite();

        cardinalityEstimatorResolver.resolve(s)
                .ifPresent(ce -> props.setCardinality(ce.getCardinality(expr)));

        costEstimatorResolver.resolve(s)
                .ifPresent(ce -> props.setCost(ce.getCost(expr)));

        Plan p = new Plan(expr);
        p.setProperties(props);

        return p;
    }
}
