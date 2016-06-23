package org.semagrow.estimator;

import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.semagrow.selector.Site;

import java.util.Optional;

/**
 * Created by angel on 15/6/2016.
 */
public abstract class AbstractCardinalityEstimator implements CardinalityEstimator, CardinalityEstimatorResolver {

    private CardinalityEstimatorResolver resolver;

    public AbstractCardinalityEstimator(CardinalityEstimatorResolver resolver) {
        assert resolver != null;
        this.resolver = resolver;
    }


    public Optional<CardinalityEstimator> resolve(Site s) {
        return this.resolver.resolve(s);
    }

    public abstract long getCardinality(TupleExpr expr);

}
