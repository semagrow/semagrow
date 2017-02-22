package org.semagrow.statistics;

import org.semagrow.selector.Site;

import java.util.Optional;

/**
 * Similar to the StatisticsFactory (i.e., Factory pattern: creates instances of the same type)
 * Created by angel on 15/6/2016.
 */
public interface StatisticsProvider {

    Optional<Statistics> getStatistics(Site site);

}
