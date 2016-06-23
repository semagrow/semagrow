package org.semagrow.statistics;

import org.semagrow.selector.Site;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by angel on 15/6/2016.
 */
public class CachedStatisticsProvider implements StatisticsProvider {

    private Map<Site,Statistics> cache = new HashMap<>();
    private StatisticsProvider provider;

    public CachedStatisticsProvider(StatisticsProvider provider) {
        this.provider = provider;
    }

    @Override
    public Optional<Statistics> getStatistics(Site site) {
        return Optional.ofNullable(cache.getOrDefault(site,null)).map(Optional::of)
                .orElseGet(() -> {
                    Optional<Statistics> stats = provider.getStatistics(site);
                    return stats
                            .map(s -> { Statistics ss = new CachedStatistics(s); cache.put(site, ss); return ss; });
                });
    }

}
