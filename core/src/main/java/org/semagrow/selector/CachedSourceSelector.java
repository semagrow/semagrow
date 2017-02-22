package org.semagrow.selector;

import org.semagrow.art.Loggable;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Collection;
import java.util.Map;

/**
 * Wraps a underlying @{link SourceSelector} and provides a simple
 * caching mechanism to tha source selection requests
 * @author Angelos Charalambidis
 */
public class CachedSourceSelector extends SourceSelectorWrapper
{
    private Map<StatementPattern, Collection<SourceMetadata>> cache = new HashMap<>();

    public CachedSourceSelector(SourceSelector selector) {
        super(selector);
    }

    @Override
    @Loggable
    public Collection<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings)
    {
        Collection<SourceMetadata> retv;

        if( cache.containsKey(pattern) ) {
            retv = cache.get(pattern);
        }
        else {
            Collection<SourceMetadata> list = super.getSources(pattern, dataset, bindings);
            cache.put(pattern, list);
            retv = list;
        }

        return retv;
    }

    private Collection<SourceMetadata> getSources(Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings)
    {
        Collection<SourceMetadata> list = new LinkedList<SourceMetadata>();
        for (StatementPattern p : patterns) {
            list.addAll(this.getSources(p, dataset, bindings));
        }
        return list;
    }

    @Override
    public Collection<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings) {
        //FIXME: This is not the case in general but only in a pattern-wise src selector
        Collection<StatementPattern> patterns  = StatementPatternCollector.process( expr );
        return getSources( patterns, dataset, bindings );
    }


}
