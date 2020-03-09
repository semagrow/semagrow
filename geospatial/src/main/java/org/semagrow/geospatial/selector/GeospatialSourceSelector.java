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

public class GeospatialSourceSelector extends SourceSelectorWrapper implements QueryAwareSourceSelector  {

    private ValueFactory vf = SimpleValueFactory.getInstance();
    private IRI HAS_GEOMETRY = vf.createIRI(GEO.NAMESPACE, "hasGeometry");

    private BoundingBoxBase boundingBoxBase = new BoundingBoxBase();
    private Map<StatementPattern,Collection<SourceMetadata>> selectorMap = new HashMap<>();
    private boolean processed = false;

    public GeospatialSourceSelector(SourceSelector selector) {
        super(selector);
    }

    public void setMetadataRepository(Repository metadata) {
        boundingBoxBase.setMetadata(metadata);
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

        for (StatementPattern patternAsWKT: patterns) {
            if (patternAsWKT.getPredicateVar().hasValue() && patternAsWKT.getPredicateVar().getValue().equals(GEO.AS_WKT)) {
                /* we found a ?s geo:asWKT ?o */

                Collection<SourceMetadata> candidateSources = getWrappedSelector().getSources(patternAsWKT, null, EmptyBindingSet.getInstance());
                Set<SourceMetadata> prunedSources = new HashSet<>();

                /* search for corresponding filter */
                for (ValueExpr filter: filters) {
                    Set<String> filterVars = VarNameCollector.process(filter);
                    String varName = patternAsWKT.getObjectVar().getName();

                    if (filterVars.contains(varName) && filterVars.size() == 1) {
                        for (SourceMetadata source: candidateSources) {
                            Literal boundingBox = boundingBoxBase.getDatasetBoundingBox(endpointOfSource(source));
                            if (boundingBox != null) {
                               if (BoundingBoxPruner.prune(filter, varName, boundingBox)) {
                                   prunedSources.add(source);
                               }
                            }
                        }
                    }
                }
                Set<SourceMetadata> sources = new HashSet<>();
                sources.addAll(candidateSources);
                sources.removeAll(prunedSources);

                selectorMap.put(patternAsWKT, sources);

                /* search for corresponding hasGeometry pattern */
                for (StatementPattern patternHasGeometry: patterns) {
                    if (patternHasGeometry.getPredicateVar().hasValue() &&
                            patternHasGeometry.getPredicateVar().getValue().equals(HAS_GEOMETRY) &&
                            patternHasGeometry.getObjectVar().getName().equals(patternAsWKT.getSubjectVar().getName())) {
                        selectorMap.put(patternHasGeometry, sources);
                    }
                }
            }
        }
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
