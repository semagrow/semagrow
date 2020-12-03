package org.semagrow.geospatial.selector;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.algebra.helpers.VarNameCollector;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.repository.Repository;
import org.semagrow.plan.util.FilterCollector;
import org.semagrow.selector.QueryAwareSourceSelector;
import org.semagrow.selector.SourceMetadata;
import org.semagrow.selector.SourceSelector;
import org.semagrow.selector.SourceSelectorWrapper;

import java.util.*;
import java.util.stream.Collectors;

public class GeospatialSourceSelector extends SourceSelectorWrapper implements QueryAwareSourceSelector {

    private BBoxBase bBoxBase = new BBoxBase();
    private SetMultimap<StatementPattern,SourceMetadata> selectorMap = HashMultimap.create();
    private boolean processed = false;

    public GeospatialSourceSelector(SourceSelector selector) {
        super(selector);
    }

    public void setMetadataRepository(Repository metadata) {
        bBoxBase.setMetadata(metadata);
    }

    @Override
    public void processTupleExpr(TupleExpr expr) {
        if (getWrappedSelector() instanceof QueryAwareSourceSelector) {
            ((QueryAwareSourceSelector) getWrappedSelector()).processTupleExpr(expr);
        }
        processBPG(expr);
        processed = true;
    }

    private void processBPG(TupleExpr expr) {

        Collection<StatementPattern> patterns = StatementPatternCollector.process(expr);
        Collection<ValueExpr> filters = FilterCollector.process(expr);

        SetMultimap<String, StatementPattern> var_patterns = HashMultimap.create();
        SetMultimap<StatementPattern, SourceMetadata> pattern_sources = HashMultimap.create();
        Map<SourceMetadata, Literal> source_bbox = new HashMap<>();

        /* initialize maps */

        for (StatementPattern p: patterns) {

            if (!p.getSubjectVar().hasValue()) {
                var_patterns.put(p.getSubjectVar().getName(), p);
            }
            if (!p.getObjectVar().hasValue()) {
                var_patterns.put(p.getObjectVar().getName(), p);
            }

            Collection<SourceMetadata> sp = getWrappedSelector().getSources(p, null, EmptyBindingSet.getInstance());
            pattern_sources.putAll(p, sp);

            for (SourceMetadata s: sp) {
                if (!source_bbox.containsKey(s)) {
                    Literal bbox = bBoxBase.getDatasetBoundingBox(endpointOfSource(s));
                    source_bbox.put(s, bbox);
                }
            }
        }

        /* prune sources for asWKT patterns according to (geospatial) filters  */

        boolean c;

        do {
            c = false;
            for (ValueExpr f : filters) {
                if (!BBoxSourcePruner.isGeospatialFilter(f)) {
                    continue;
                }
                Collection<String> vars = VarNameCollector.process(f);
                Iterator<String> iter = vars.iterator();

                if (vars.size() == 1) {
                    String v = iter.next();
                    StatementPattern wktPattern = getAsWktPattern(v, var_patterns);

                    Set<SourceMetadata> toremove = new HashSet<>();
                    for (SourceMetadata s: pattern_sources.get(wktPattern)) {
                        if (BBoxSourcePruner.emptyResultSet(f, v, source_bbox.get(s))) {
                            c = true;
                            toremove.add(s);
                        }
                    }
                    for (SourceMetadata s: toremove) {
                        pattern_sources.remove(wktPattern, s);
                    }
                }

                if (vars.size() == 2) {
                    String v1 = iter.next();
                    String v2 = iter.next();
                    StatementPattern wktPattern1 = getAsWktPattern(v1, var_patterns);
                    StatementPattern wktPattern2 = getAsWktPattern(v2, var_patterns);
                    Set<SourceMetadata> toRemove;

                    toRemove = new HashSet<>();
                    for (SourceMetadata s1: pattern_sources.get(wktPattern1)) {
                        boolean empty = true;
                        for (SourceMetadata s2: pattern_sources.get(wktPattern2)) {
                            if (!BBoxSourcePruner.emptyResultSet(f, v1, source_bbox.get(s1), v2, source_bbox.get(s2))) {
                                empty = false;
                            }
                        }
                        if (empty) {
                            c = true;
                            toRemove.add(s1);
                        }
                    }
                    for (SourceMetadata s: toRemove) {
                        pattern_sources.remove(wktPattern1, s);
                    }

                   toRemove = new HashSet<>();
                    for (SourceMetadata s2: pattern_sources.get(wktPattern2)) {
                        boolean empty = true;
                        for (SourceMetadata s1: pattern_sources.get(wktPattern1)) {
                            if (!BBoxSourcePruner.emptyResultSet(f, v1, source_bbox.get(s1), v2, source_bbox.get(s2))) {
                                empty = false;
                            }
                        }
                        if (empty) {
                            c = true;
                            toRemove.add(s2);
                        }
                    }
                    for (SourceMetadata s: toRemove) {
                        pattern_sources.remove(wktPattern2, s);
                    }
                }
            }
        } while (c);

        /* update selector map */
        selectorMap.putAll(pattern_sources);
    }

    private Resource endpointOfSource(SourceMetadata source) {
        return source.getSites().iterator().next().getID();
    }

    private StatementPattern getAsWktPattern(String var, SetMultimap<String, StatementPattern> var_patterns) {
        StatementPattern wktPattern = var_patterns.get(var).stream()
                .filter(p -> p.getPredicateVar().hasValue() && p.getPredicateVar().getValue().equals(GEO.AS_WKT))
                .collect(Collectors.toList()).get(0);
        return wktPattern;
    }

    @Override
    public Collection<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings)
    {
        if (processed) {
            return selectorMap.get(pattern);
        }
        return super.getWrappedSelector().getSources(pattern, dataset, bindings);
    }

    private Collection<SourceMetadata> getSources(Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings) {
        Collection<SourceMetadata> list = new LinkedList<SourceMetadata>();
        for(StatementPattern p : patterns) {
            list.addAll(this.getSources(p, dataset, bindings));
        }
        return list;
    }

    @Override
    public Collection<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings)  {
        if(expr instanceof StatementPattern) {
            return getSources((StatementPattern)expr, dataset, bindings);
        }

        Collection<StatementPattern> patterns  = StatementPatternCollector.process(expr);
        return getSources(patterns, dataset, bindings);
    }
}
