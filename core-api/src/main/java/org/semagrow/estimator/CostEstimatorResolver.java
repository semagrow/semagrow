package org.semagrow.estimator;

import org.semagrow.selector.Site;

import java.util.Optional;

/**
 * Created by angel on 15/6/2016.
 */
public interface CostEstimatorResolver {

    Optional<CostEstimator> resolve(Site site);

}
