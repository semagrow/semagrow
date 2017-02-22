package org.semagrow.estimator;

import org.semagrow.selector.Site;
import org.semagrow.statistics.StatisticsProvider;

import java.util.Optional;

/**
 * Created by angel on 15/6/2016.
 */
public class SimpleSelectivityEstimatorResolver implements SelectivityEstimatorResolver {

    private StatisticsProvider statisticsProvider;

    public SimpleSelectivityEstimatorResolver(StatisticsProvider statisticsProvider) {
        this.statisticsProvider = statisticsProvider;
    }

    public Optional<SelectivityEstimator> resolve(Site site) {
        return statisticsProvider.getStatistics(site)
                .map(stats -> new SimpleSelectivityEstimator(this, stats));
    }
}