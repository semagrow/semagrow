package eu.semagrow.stack.modules.querydecomp.selector;

import eu.semagrow.stack.modules.querydecomp.SourceMetadata;
import eu.semagrow.stack.modules.querydecomp.SourceSelector;
import eu.semagrow.stack.modules.vocabulary.VOID;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.*;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by angel on 5/27/14.
 */
public class VOIDSourceSelector implements SourceSelector {

    private Repository voidRepository;

    private final String PREFIX = "PREFIX void: " + VOID.NS.toString() + "\n";

    public VOIDSourceSelector(Repository voidRepository) {
        this.voidRepository = voidRepository;
    }

    public List<SourceMetadata> getSources(StatementPattern pattern) {
        Value sVal = pattern.getSubjectVar().getValue();
        Value oVal = pattern.getSubjectVar().getValue();
        Value pVal = pattern.getPredicateVar().getValue();

        if (pVal != null) {
            String s = "SELECT DISTINCT ?endpoint { " +
                    "[] <" + VOID.PROPERTY + "> ?property ; " +
                    "   <" + VOID.SPARQLENDPOINT +"> ?endpoint . }";

            System.out.println(s);
            RepositoryConnection conn = null;

            try {
                conn = voidRepository.getConnection();
                TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, s);
                q.setIncludeInferred(true);
                q.setBinding("property", pVal);
                TupleQueryResult result = q.evaluate();
                return createList(result);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (conn != null)
                    try { conn.close(); }catch(Exception e){ }
            }
        }
        return new ArrayList();
    }


    /**
     *
     * @param pattern
     * @return a collection of dataset names that contain matching triples
     */
    private Collection<Value> getDatasets(StatementPattern pattern) {
        return null;
    }

    private List<SourceMetadata> createList(TupleQueryResult result)
            throws QueryEvaluationException {

        List<SourceMetadata> list = new LinkedList<SourceMetadata>();
        while (result.hasNext()) {
            BindingSet b = result.next();
            URI endpoint = (URI) b.getBinding("endpoint").getValue();
            list.add(createSourceMetadata(endpoint));
        }
        return list;
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
