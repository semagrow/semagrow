package org.semagrow.geospatial.selector;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
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
import org.semagrow.plan.util.BPGCollector;
import org.semagrow.plan.util.FilterCollector;
import org.semagrow.selector.QueryAwareSourceSelector;
import org.semagrow.selector.SourceMetadata;
import org.semagrow.selector.SourceSelector;
import org.semagrow.selector.SourceSelectorWrapper;

import java.util.*;
import java.util.stream.Collectors;

public class SimpleGeospatialSourceSelector extends SourceSelectorWrapper implements QueryAwareSourceSelector  {

    private ValueFactory vf = SimpleValueFactory.getInstance();
    private IRI HAS_GEOMETRY = vf.createIRI(GEO.NAMESPACE, "hasGeometry");

    private BBoxBase bBoxBase = new BBoxBase();
    private Map<StatementPattern,Collection<SourceMetadata>> selectorMap = new HashMap<>();
    private boolean processed = false;

    public SimpleGeospatialSourceSelector(SourceSelector selector) {
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
        Collection<TupleExpr> bpgs = BPGCollector.process(expr);
        for (TupleExpr bpg: bpgs) {
            processBPG(bpg);
        }
        processed = true;
    }

    private void processBPG(TupleExpr expr) {

        Collection<StatementPattern> patterns = StatementPatternCollector.process(expr);
        Collection<ValueExpr> filters = FilterCollector.process(expr);
        //Map<String,Set<Resource>> hasGeoMap = new HashMap<>();

        for (StatementPattern pattern: patterns) {
            if (pattern.getPredicateVar().hasValue() && pattern.getPredicateVar().getValue().equals(GEO.AS_WKT)) {
                /* we found a ?s geo:asWKT ?o */

                Collection<SourceMetadata> candidateSources = getWrappedSelector().getSources(pattern, null, EmptyBindingSet.getInstance());
                Set<SourceMetadata> prunedSources = new HashSet<>();
                Set<Resource> endpoints = candidateSources.stream().map(s -> endpointOfSource(s)).collect(Collectors.toSet());

                /* search for corresponding filter */
                for (ValueExpr filter: filters) {
                    Set<String> filterVars = VarNameCollector.process(filter);
                    String varName = pattern.getObjectVar().getName();

                    if (filterVars.contains(varName) && filterVars.size() == 1) {
                        for (SourceMetadata source: candidateSources) {
                            Literal boundingBox = bBoxBase.getDatasetBoundingBox(endpointOfSource(source));
                            if (boundingBox != null) {
                               if (BBoxSourcePruner.emptyResultSet(filter, varName, boundingBox)) {
                                   prunedSources.add(source);
                                   endpoints.remove(endpointOfSource(source));
                               }
                            }
                        }
                    }
                }
                Set<SourceMetadata> sources = new HashSet<>();
                sources.addAll(candidateSources);
                sources.removeAll(prunedSources);

                selectorMap.put(pattern, sources);
                //hasGeoMap.put(pattern.getSubjectVar().getName(), endpoints);
            }
        }

        /* search for corresponding hasGeometry patterns */
        /*
        for (StatementPattern pattern: patterns) {
            if (pattern.getPredicateVar().hasValue() && pattern.getPredicateVar().getValue().equals(HAS_GEOMETRY)) {
                // we found a ?s geo:hasGeometry ?g

                Collection<SourceMetadata> candidateSources = getWrappedSelector().getSources(pattern, null, EmptyBindingSet.getInstance());
                Set<SourceMetadata> prunedSources = new HashSet<>();

                for (SourceMetadata source: candidateSources) {
                    if (hasGeoMap.get(pattern.getObjectVar().getName()).contains(endpointOfSource(source))) {
                        prunedSources.add(source);
                    }
                }
                selectorMap.put(pattern, prunedSources);
            }
        }
        */
    }

    private Resource endpointOfSource(SourceMetadata source) {
        return source.getSites().iterator().next().getID();
    }

    @Override
    public Collection<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings)
    {
        if (processed && selectorMap.containsKey(pattern)) {
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
