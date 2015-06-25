package eu.semagrow.core.impl.selector;

import eu.semagrow.core.statistics.Statistics;
import eu.semagrow.core.statistics.StatisticsProvider;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.StatementPattern;

import java.util.HashMap;

/**
 * Created by angel on 7/5/2015.
 */
public class CachedStatisticsProvider implements StatisticsProvider
{
    private StatisticsProvider provider;

    private HashMap<StatementPattern, HashMap<URI, Statistics>> cache = new HashMap<>();

    public CachedStatisticsProvider(StatisticsProvider provider)
    {
        this.provider = provider;
    }

    @Override
    public long getTriplesCount(URI source) {
        return provider.getTriplesCount(source);
    }

    @Override
    public Statistics getStats(StatementPattern pattern, BindingSet bindings, URI source) {
        Statistics stats = getCached(pattern, bindings, source);
        if (stats == null) {
            stats = provider.getStats(pattern, bindings, source);
            cache(pattern, bindings, source, stats);
            return stats;
        }
        return stats;
    }

    private Statistics getCached(StatementPattern pattern, BindingSet bindings, URI source) {
        HashMap<URI, Statistics> h = cache.getOrDefault(pattern, new HashMap<URI, Statistics>());
        return h.getOrDefault(source,null);
    }


    private void cache(StatementPattern pattern, BindingSet bindings, URI source, Statistics stats) {
        HashMap<URI, Statistics> h = cache.getOrDefault(pattern, new HashMap<URI,Statistics>());
        h.put(source,stats);
        cache.put(pattern, h);
    }
}
