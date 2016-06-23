package org.semagrow.estimator;

import org.semagrow.selector.Site;

import java.util.Optional;

/**
 * Created by angel on 15/6/2016.
 */
public class SimpleCostEstimatorResolver implements CostEstimatorResolver {

    private CardinalityEstimatorResolver cardinalityEstimatorResolver;

    public SimpleCostEstimatorResolver(CardinalityEstimatorResolver cardinalityEstimatorResolver) {
        this.cardinalityEstimatorResolver = cardinalityEstimatorResolver;
    }

    @Override
    public Optional<CostEstimator> resolve(Site site) {
        return cardinalityEstimatorResolver.resolve(site)
                .map(c -> new SimpleCostEstimator(this, c));
    }
}
