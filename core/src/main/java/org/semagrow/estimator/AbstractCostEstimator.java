package org.semagrow.estimator;

import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.semagrow.plan.Cost;
import org.semagrow.plan.operators.SourceQuery;
import org.semagrow.selector.Site;

import java.util.Optional;

/**
 * Created by angel on 15/6/2016.
 */
public abstract class AbstractCostEstimator implements CostEstimator, CostEstimatorResolver {

    private CostEstimatorResolver resolver;

    public AbstractCostEstimator(CostEstimatorResolver resolver) {
        assert resolver != null;
        this.resolver = resolver;
    }

    public Optional<CostEstimator> resolve(Site s) {
        return resolver.resolve(s);
    }

    public abstract Cost getCost(TupleExpr expr); /* {
        if (expr instanceof SourceQuery) {
            return getCost((SourceQuery)expr);
        }
    }

    public abstract Cost getCost(SourceQuery expr);
    */

}
