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
public class VOIDSourceSelector implements SourceSelector {

    private Repository voidRepository;

    public VOIDSourceSelector(Repository voidRepository) {
        this.voidRepository = voidRepository;
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

    private Set<Resource> getMatchingDatasetsOfPredicate(URI pred) {
        String q = "SELECT ?dataset { ?dataset <" + VOID.PROPERTY + "> ?prop. }";
        //String q = "SELECT ?dataset { ?dataset ?p ?p1. }";
        QueryBindingSet bindings = new QueryBindingSet();
        bindings.addBinding("prop", pred);
        return createSet(evalQuery(q, bindings), "dataset");
    }

    private Set<Resource> getMatchingDatasetsOfSubject(URI subject) {
        String q = "SELECT ?dataset { ?dataset <" + VOID.URIREGEXPATTERN + "> ?pattern . FILTER regex(?). }";
        QueryBindingSet bindings = new QueryBindingSet();
        bindings.addBinding("subject", subject);
        return createSet(evalQuery(q, bindings), "dataset");
    }

    private Set<Resource> createSet(TupleQueryResult result, String binding) {
        Set<Resource> set = new HashSet<Resource>();

        if (result == null)
            return set;

        try {
            while (result.hasNext()) {
                Value v = result.next().getBinding(binding).getValue();
                if (v instanceof Resource) {
                    set.add((Resource) v);
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        return set;
    }

    private TupleQueryResult evalQuery(String queryString, BindingSet bindingSet) {
        RepositoryConnection conn = null;
        try {
            conn = voidRepository.getConnection();
            TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            q.setIncludeInferred(true);
            //for (Binding b : bindingSet)
            //    q.setBinding(b.getName(), b.getValue());
            return q.evaluate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
        return null;
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

    private URI getEndpoint(Resource dataset) {
        String qStr = "SELECT ?endpoint { ?dataset <" + VOID.SPARQLENDPOINT + "> ?endpoint }";
        RepositoryConnection conn = null;
        try {
            conn = voidRepository.getConnection();
            TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, qStr);
            q.setIncludeInferred(true);
            q.setBinding("dataset", dataset);
            TupleQueryResult r = q.evaluate();
            if (!r.hasNext())
                return null;
            else
                return (URI)r.next().getBinding("endpoint").getValue();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null)
                try { conn.close(); }catch(Exception e){ }
        }
        return null;
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
