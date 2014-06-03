package eu.semagrow.stack.modules.querydecomp.selector;

import eu.semagrow.stack.modules.querydecomp.SourceMetadata;
import eu.semagrow.stack.modules.querydecomp.SourceSelector;
import eu.semagrow.stack.modules.vocabulary.VOID;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.*;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.ListBindingSet;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

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

    public List<SourceMetadata> getSources(StatementPattern pattern) {
        return new LinkedList(datasetsToSourceMetadata(getDatasets(pattern)));
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

        return datasets;
    }

    private Collection<SourceMetadata> datasetsToSourceMetadata(
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
            metadata.add(createSourceMetadata(endpoint));
        }

        return metadata;
    }

    private SourceMetadata createSourceMetadata(final URI endpoint) {
        return new SourceMetadata() {
            public List<URI> getEndpoints() {
                List<URI> endpoints = new ArrayList<URI>(1);
                endpoints.add(endpoint);
                return endpoints;
            }

            public StatementPattern originalPattern() {
                return null;
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
