package org.semagrow.estimator;

import org.semagrow.selector.Site;

import java.util.Optional;

/**
 * The interface of a resolver of {@link SelectivityEstimator}s
 * @author acharal
 * @since 2.0
 */
public interface SelectivityEstimatorResolver {

    /**
     * Selects the appropriate {@link SelectivityEstimator} for a specific {@code site}
     * @param site a given {@link Site}
     * @return an object that implements the {@link SelectivityEstimator}
     *         or nothing if there is nothing appropriate for the given {@code site}
     */
    Optional<SelectivityEstimator> resolve(Site site);

}
