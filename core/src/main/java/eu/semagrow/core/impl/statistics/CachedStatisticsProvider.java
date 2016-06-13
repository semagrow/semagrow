package eu.semagrow.core.impl.statistics;

import eu.semagrow.core.statistics.Statistics;
import eu.semagrow.core.statistics.StatisticsProvider;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

import java.util.HashMap;

/**
 * Created by angel on 7/5/2015.
 */
public class CachedStatisticsProvider implements StatisticsProvider
{
    private StatisticsProvider provider;

    private HashMap<StatementPattern, HashMap<IRI, Statistics>> cache = new HashMap<>();

    public CachedStatisticsProvider(StatisticsProvider provider)
    {
        this.provider = provider;
    }

    @Override
    public long getTriplesCount(IRI source) {
        return provider.getTriplesCount(source);
    }

    @Override
    public Statistics getStats(StatementPattern pattern, BindingSet bindings, IRI source) {
        Statistics stats = getCached(pattern, bindings, source);
        if (stats == null) {
            stats = provider.getStats(pattern, bindings, source);
            cache(pattern, bindings, source, stats);
            return stats;
        }
        return stats;
    }

    private Statistics getCached(StatementPattern pattern, BindingSet bindings, IRI source) {
        HashMap<IRI, Statistics> h = cache.getOrDefault(pattern, new HashMap<IRI, Statistics>());
        return h.getOrDefault(source,null);
    }


    private void cache(StatementPattern pattern, BindingSet bindings, IRI source, Statistics stats) {
        HashMap<IRI, Statistics> h = cache.getOrDefault(pattern, new HashMap<IRI,Statistics>());
        h.put(source,stats);
        cache.put(pattern, h);
    }
}
