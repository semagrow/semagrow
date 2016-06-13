package eu.semagrow.core.impl.selector;

import eu.semagrow.art.Loggable;
import eu.semagrow.core.impl.sparql.SPARQLSite;
import eu.semagrow.core.impl.sparql.SPARQLSiteFactory;
import eu.semagrow.core.source.*;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.repository.Repository;

import java.util.*;

/**
 * The default source selection implementation based on a Repository
 * that uses VoID description to describe dataset metadata
 * @author Angelos Charalambidis
 */
public class VOIDSourceSelector extends VOIDBase
        implements SourceSelector {

    private Repository voidRepository;

    public VOIDSourceSelector(Repository voidRepository) {
        super(voidRepository);
    }

    @Loggable
    public List<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {

        if (!pattern.getSubjectVar().hasValue() &&
            !pattern.getPredicateVar().hasValue() &&
            !pattern.getObjectVar().hasValue())
            return new LinkedList<SourceMetadata>(uritoSourceMetadata(pattern, getEndpoints()));
        else
            return new LinkedList<SourceMetadata>(datasetsToSourceMetadata(pattern, getDatasets(pattern)));
    }

    @Loggable
    public List<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings) {

        if (expr instanceof StatementPattern)
            return getSources((StatementPattern)expr, dataset, bindings);

        List<StatementPattern> patterns = StatementPatternCollector.process(expr);
        return getSources(patterns, dataset, bindings);
    }

    public List<SourceMetadata> getSources(Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings) {
        List<SourceMetadata> metadata = new LinkedList<SourceMetadata>();
        for (StatementPattern pattern : patterns) {
            metadata.addAll(getSources(pattern, dataset, bindings));
        }
        return metadata;
    }

    /**
     *
     * @param pattern
     * @return a collection of dataset names that contain matching triples
     */
    private Set<Resource> getDatasets(StatementPattern pattern) {
        Value sVal = pattern.getSubjectVar().getValue();
        Value oVal = pattern.getObjectVar().getValue();
        Value pVal = pattern.getPredicateVar().getValue();

        Set<Resource> datasets = new HashSet<Resource>();

        if (pVal != null && pVal instanceof IRI)
            datasets.addAll(getMatchingDatasetsOfPredicate((IRI)pVal));

        if (sVal != null && sVal instanceof IRI)
            datasets.addAll(getMatchingDatasetsOfSubject((IRI)sVal));

        if (oVal != null && oVal instanceof IRI)
            datasets.addAll(getMatchingDatasetsOfObject((IRI)oVal));

        return datasets;
    }

    private Collection<SourceMetadata> datasetsToSourceMetadata(StatementPattern pattern,
            Collection<Resource> datasets) {

        Set<IRI> endpoints = new HashSet<IRI>();
        for (Resource dataset : datasets) {
            IRI endpoint = getEndpoint(dataset);
            if (endpoint != null) {
                endpoints.add(endpoint);
            }
        }

        if (endpoints.isEmpty()) {
            endpoints = getEndpoints();
        }

        Collection<SourceMetadata> metadata = new LinkedList<SourceMetadata>();
        for (IRI endpoint : endpoints) {
            metadata.add(createSourceMetadata(pattern, endpoint));
        }

        return metadata;
    }

    private Collection<SourceMetadata> uritoSourceMetadata(StatementPattern pattern, Collection<IRI> endpoints) {
        Collection<SourceMetadata> metadata = new LinkedList<SourceMetadata>();
        for (IRI e : endpoints) {
            metadata.add(createSourceMetadata(pattern, e));
        }
        return metadata;
    }

    private SourceMetadata createSourceMetadata(final StatementPattern pattern, final IRI endpoint) {

        return new SourceMetadataImpl(pattern, endpoint);
    }


    private class SourceMetadataImpl implements SourceMetadata {

        private List<Site> endpoints = new LinkedList<Site>();

        private StatementPattern pattern;

        private Map<String, Collection<IRI>> schemaMappings;

        public SourceMetadataImpl(StatementPattern pattern, IRI endpoint) {
            this.pattern = pattern;
            SiteFactory siteFactory;
            if (endpoint.stringValue().contains("cassandra")) {
                siteFactory = SiteRegistry.getInstance().get("CASSANDRA").get();
            } else {

                siteFactory = SiteRegistry.getInstance().get("SPARQL").get();
            }
            //FIXME (not appropriate sitefactory)
            endpoints.add(siteFactory.getSite(endpoint));
            schemaMappings = new HashMap<String, Collection<IRI>>();
        }

        public List<Site> getSites() { return endpoints; }

        public StatementPattern original() {
            return pattern;
        }

        public StatementPattern target() { return pattern; }

        public boolean isTransformed() {
            return false;
        }

        public double getSemanticProximity() {
            return 1.0;
        }

        public Collection<IRI> getSchema(String var) {
            if (schemaMappings.containsKey(var)) {
                return schemaMappings.get(var);
            }
            return Collections.emptySet();
        }
    }
}
