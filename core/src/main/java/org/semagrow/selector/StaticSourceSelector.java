package org.semagrow.selector;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import java.util.*;

/**
 * A source selector that is constructed with a list of {@link SourceMetadata}
 * and then serve SourceMetadata only of this list.
 */
public class StaticSourceSelector implements SourceSelector {

    private Map<StatementPattern, Collection<SourceMetadata>> map = new HashMap<>();

    public StaticSourceSelector(Collection<SourceMetadata> list) {
        buildMap(list);
    }

    private void buildMap(Collection<SourceMetadata> list) {
        for (SourceMetadata l : list) {
            Collection<SourceMetadata> ll = new LinkedList<SourceMetadata>();
            if (map.containsKey(l.original()))
                ll = map.get(l.original());

            ll.add(l);
            map.put(l.original(), ll);
        }
    }

    @Override
    public void setSiteResolver(SiteResolver siteResolver) {

    }

    @Override
    public Collection<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {
        if (map.containsKey(pattern))
            return map.get(pattern);
        else
            return Collections.emptyList();
    }

    @Override
    public Collection<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings) {
        if (expr instanceof StatementPattern)
            return getSources((StatementPattern) expr,dataset,bindings);
        else
            return Collections.emptyList();
    }
}
