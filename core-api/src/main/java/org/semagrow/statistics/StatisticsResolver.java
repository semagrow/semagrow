package org.semagrow.statistics;

import org.semagrow.selector.Site;

import java.util.Optional;

/**
 * Created by angel on 19/6/2016.
 */
public interface StatisticsResolver {

    Optional<Statistics> getStatistics(Site site);

}
