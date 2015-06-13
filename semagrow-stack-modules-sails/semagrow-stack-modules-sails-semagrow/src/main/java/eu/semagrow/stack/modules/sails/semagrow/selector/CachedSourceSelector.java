package eu.semagrow.stack.modules.sails.semagrow.selector;

import eu.semagrow.stack.modules.api.source.SourceMetadata;
import eu.semagrow.stack.modules.api.source.SourceSelector;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by angel on 10/6/2015.
 */
public class CachedSourceSelector extends SourceSelectorWrapper {


    private Map<StatementPattern, List<SourceMetadata>> cache = new HashMap<>();

    public CachedSourceSelector(SourceSelector selector) {
        super(selector);
    }

    @Override
    public List<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {
        if (cache.containsKey(pattern)) {
            return cache.get(pattern);
        } else {
            List<SourceMetadata> list = super.getSources(pattern, dataset, bindings);
            cache.put(pattern, list);
            return list;
        }
    }

    @Override
    public List<SourceMetadata> getSources(Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings) {
        List<SourceMetadata> list = new LinkedList<SourceMetadata>();
        for (StatementPattern p : patterns) {
            list.addAll(this.getSources(p, dataset, bindings));
        }
        return list;
    }


}
