package org.semagrow.estimator;

import org.semagrow.selector.Site;

import java.util.Optional;

/**
 * The interface of a resolver of {@link CostEstimator}s
 * @author acharal
 * @since 2.0
 */
public interface CostEstimatorResolver {

    /**
     * Selects the appropriate {@link CostEstimator} for a specific {@code site}
     * @param site a given {@link Site}
     * @return an object that implements the {@link CostEstimator}
     *         or nothing if there is nothing appropriate for the given {@code site}
     */
    Optional<CostEstimator> resolve(Site site);

}
