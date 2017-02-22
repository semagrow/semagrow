package org.semagrow.estimator;

import org.semagrow.selector.Site;
import org.semagrow.statistics.StatisticsProvider;

import java.util.Optional;

/**
 * Created by angel on 15/6/2016.
 */
public class SimpleCardinalityEstimatorResolver implements CardinalityEstimatorResolver {

    private StatisticsProvider statisticsProvider;
    private SelectivityEstimatorResolver selectivityEstimatorResolver;

    public SimpleCardinalityEstimatorResolver(StatisticsProvider statisticsProvider, SelectivityEstimatorResolver selectivityEstimatorResolver) {
        this.statisticsProvider = statisticsProvider;
        this.selectivityEstimatorResolver = selectivityEstimatorResolver;
    }

    @Override
    public Optional<CardinalityEstimator> resolve(Site site) {
        return selectivityEstimatorResolver.resolve(site)
                    .flatMap(sel -> statisticsProvider.getStatistics(site)
                                        .map(stats -> new SimpleCardinalityEstimator(this, sel, stats)));
    }

}
