package org.semagrow.statistics;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

import java.util.HashMap;
import java.util.Optional;

/**
 * Created by angel on 7/5/2015.
 */
public class CachedStatistics implements Statistics {
    private Statistics provider;

    private HashMap<StatementPattern, StatsItem> cache = new HashMap<>();

    public CachedStatistics(Statistics provider) {
        this.provider = provider;
    }

    @Override
    public long getTriplesCount() {
        return provider.getTriplesCount();
    }

    @Override
    public StatsItem getStats(StatementPattern pattern, BindingSet bindings) {
        return getCached(pattern, bindings)
                .orElseGet(() -> {
                    StatsItem s = provider.getStats(pattern, bindings);
                    cache(pattern, bindings, s);
                    return s;
                });
    }

    private Optional<StatsItem> getCached(StatementPattern pattern, BindingSet bindings) {
        return Optional.ofNullable(cache.getOrDefault(pattern, null));
    }


    private void cache(StatementPattern pattern, BindingSet bindings, StatsItem stats) {
        cache.put(pattern, stats);
    }
}
