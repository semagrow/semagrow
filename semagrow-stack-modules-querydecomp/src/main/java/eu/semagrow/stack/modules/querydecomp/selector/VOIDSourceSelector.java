package eu.semagrow.stack.modules.querydecomp.selector;

import eu.semagrow.stack.modules.api.source.SourceMetadata;
import eu.semagrow.stack.modules.api.source.SourceSelector;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.repository.Repository;

import java.util.*;

/**
 * TODO: Use properties void:uriSpace, svd:objectUriRegexPattern, ...
 * TODO: Use alternative ``mirror'' endpoints.
 * TODO: Use transformed pattern
 * Created by angel on 5/27/14.
 */
public class VOIDSourceSelector extends VOIDBase
        implements SourceSelector {

    private Repository voidRepository;

    public VOIDSourceSelector(Repository voidRepository) {
        super(voidRepository);
    }

    public List<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {

        if (!pattern.getSubjectVar().hasValue() &&
            !pattern.getPredicateVar().hasValue() &&
            !pattern.getObjectVar().hasValue())
            return new LinkedList(uritoSourceMetadata(pattern, getEndpoints()));
        else
            return new LinkedList(datasetsToSourceMetadata(pattern, getDatasets(pattern)));
    }

    public List<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings) {
        if (expr instanceof StatementPattern)
            return getSources((StatementPattern)expr, dataset, bindings);

        List<StatementPattern> patterns = StatementPatternCollector.process(expr);
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
        Value oVal = pattern.getSubjectVar().getValue();
        Value pVal = pattern.getPredicateVar().getValue();

        Set<Resource> datasets = new HashSet<Resource>();

        if (pVal != null && pVal instanceof URI)
            datasets.addAll(getMatchingDatasetsOfPredicate((URI)pVal));

        if (sVal != null && sVal instanceof URI)
            datasets.addAll(getMatchingDatasetsOfSubject((URI)sVal));

        if (oVal != null && oVal instanceof URI)
            datasets.addAll(getMatchingDatasetsOfObject((URI)oVal));

        return datasets;
    }

    private Collection<SourceMetadata> datasetsToSourceMetadata(StatementPattern pattern,
            Collection<Resource> datasets) {

        Set<URI> endpoints = new HashSet<URI>();
        for (Resource dataset : datasets) {
            URI endpoint = getEndpoint(dataset);
            if (endpoint != null) {
                endpoints.add(endpoint);
            }
        }

        Collection<SourceMetadata> metadata = new LinkedList<SourceMetadata>();
        for (URI endpoint : endpoints) {
            metadata.add(createSourceMetadata(pattern, endpoint));
        }

        return metadata;
    }

    private Collection<SourceMetadata> uritoSourceMetadata(StatementPattern pattern, Collection<URI> endpoints) {
        Collection<SourceMetadata> metadata = new LinkedList<SourceMetadata>();
        for (URI e : endpoints) {
            metadata.add(createSourceMetadata(pattern, e));
        }
        return metadata;
    }

    private SourceMetadata createSourceMetadata(final StatementPattern pattern, final URI endpoint) {
        return new SourceMetadata() {
            public List<URI> getEndpoints() {
                List<URI> endpoints = new ArrayList<URI>(1);
                endpoints.add(endpoint);
                return endpoints;
            }

            public StatementPattern originalPattern() {
                return pattern;
            }

            public boolean requiresTransform() {
                return false;
            }

            public double getSemanticProximity() {
                return 0;
            }
        };
    }
}
