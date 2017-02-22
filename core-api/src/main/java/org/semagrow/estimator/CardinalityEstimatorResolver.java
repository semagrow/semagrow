package org.semagrow.estimator;

import org.semagrow.selector.Site;

import java.util.Optional;

/**
 * The interface of a resolver of {@link CardinalityEstimator}s
 * @author acharal
 * @since 2.0
 */
public interface CardinalityEstimatorResolver {

    /**
     * Selects the appropriate {@link CardinalityEstimator} for a specific {@code site}
     * @param site a given {@link Site}
     * @return an object that implements the {@link CardinalityEstimator}
     *         or nothing if there is nothing appropriate for the given {@code site}
     */
    Optional<CardinalityEstimator> resolve(Site site);

}
