package eu.semagrow.stack.modules.querydecomp.selector;

import eu.semagrow.stack.modules.querydecomp.Statistics;
import eu.semagrow.stack.modules.vocabulary.VOID;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.*;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.openrdf.query.QueryLanguage.SPARQL;

/**
 * Created by angel on 4/30/14.
 */
public class VOIDStatistics implements Statistics {

    private Repository voidRepository;

    private static final String PREFIX =
            "PREFIX void: <" + VOID.NAMESPACE + ">\n";

    private static final String TRIPLE_COUNT = PREFIX +
            "SELECT ?count WHERE {" +
            "  [] void:sparqlEndpoint ?endpoint ;" +
            "     void:triples ?count." +
            "}";

    private static final String TRIPLE_COUNT1 = PREFIX +
            "SELECT ?count WHERE {" +
            "  [] void:sparqlEndpoint ?endpoint ;" +
            "     void:uriRegexPattern ?pattern ;" +
            "     void:triples ?count." +
            " FILTER regex(?s, ?pattern) " +
            "}";

    private static final String TRIPLE_COUNT2 = PREFIX +
            "SELECT ?count WHERE {" +
            "  [] void:sparqlEndpoint ?endpoint ;" +
            "     void:property ?p ;" +
            "     void:triples ?count." +
            "}";

    private class StatisticsStruct {
        long triples;
        long distinctSubjects;
        long distinctObjects;
        long distinctPredicates;
    }

    public VOIDStatistics(Repository voidRepository) {
        this.voidRepository = voidRepository;
    }

    protected Repository getRepository() { return voidRepository; }

    /**
     * Returns the count value defined by the supplied query and variable substitutions.
     *
     * @param query the query to be executed on the voiD repository.
     * @param vars the variable bindings to be substituted in the query.
     * @return the resulting count value.
     */
    private long getCount(String query, String... vars) {

        // replace query variables
        for (int i = 0; i < vars.length; i++) {
            query = query.replace(vars[i], vars[++i]);
        }

        List<String> bindings = evalQuery(query, "count");

        // check result validity
        if (bindings.size() == 0) {

            return -1;
        }

        return Long.parseLong(bindings.get(0));
    }

    /**
     * Evaluates the given query and returns the result values for the specified binding name.
     *
     * @param query the query to evaluate.
     * @param bindingName the desired result bindings.
     * @return a list of result binding values.
     */
    private List<String> evalQuery(String query, String bindingName) {
        try {
            RepositoryConnection con = this.getRepository().getConnection();
            try {
                TupleQuery tupleQuery = con.prepareTupleQuery(SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();

                try {
                    List<String> bindings = new ArrayList<String>();
                    while (result.hasNext()) {
                        bindings.add(result.next().getValue(bindingName).stringValue());
                    }
                    return bindings;
                } catch (QueryEvaluationException e) {

                } finally {
                    result.close();
                }
            } catch (IllegalArgumentException e) {

            } catch (RepositoryException e) {

            } catch (MalformedQueryException e) {

            } catch (QueryEvaluationException e) {

            } finally {
                con.close();
            }
        } catch (RepositoryException e) {

        }
        return null;
    }

    public long getDistinctObjects(StatementPattern pattern, URI source) {
        return 0;
    }

    public long getDistinctSubjects(StatementPattern pattern, URI source) {
        return 0;
    }

    public long getDistinctPredicates(StatementPattern pattern, URI source){
        return 0;
    }

    public long getTripleCount(URI source) {

        // get all triples statistics from datasets with property sparqlEndpoint = source
        // and get the maximum
        return 0;
    }

    private Set<Resource> getDatasetsOfSubject(URI source, Value subjectValue)
            throws QueryEvaluationException, RepositoryException {

        String queryString =
                "SELECT DISTINCT ?dataset WHERE {" +
                "?dataset " + VOID.SPARQLENDPOINT + " ?endpoint" +
                "?dataset " + VOID.URIREGEXPATTERN + " ?rp. " +
                "FILTER regex(?s, ?rp). }";

        RepositoryConnection conn = null;

        try {
            conn = voidRepository.getConnection();
            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            query.setBinding("endpoint", source);
            query.setBinding("s", subjectValue);

            return getDatasets(query.evaluate());

        } catch (RepositoryException e) {

        } catch (MalformedQueryException e) {

        } finally {
            if (conn != null)
                conn.close();
        }

        return new HashSet<Resource>();
    }

    private Set<Resource> getDatasetsOfObject(URI source, Value objectValue)
            throws QueryEvaluationException, RepositoryException {

        String queryString = "SELECT DISTINCT ?dataset WHERE {" +
                "?dataset " + VOID.SPARQLENDPOINT + " ?endpoint" +
                "?dataset svd:objectUriRegexPattern ?rp. " +
                "FILTER regex(?o, ?rp). }";

        RepositoryConnection conn = null;
        try {
            conn = voidRepository.getConnection();
            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            query.setBinding("endpoint", source);
            query.setBinding("o", objectValue);

            return getDatasets(query.evaluate());

        } catch (RepositoryException e) {

        } catch (MalformedQueryException e) {

        } finally {
            if (conn != null)
                conn.close();
        }

        return new HashSet<Resource>();
    }

    private Set<Resource> getDatasetsOfPredicate(URI source, Value propertyValue)
            throws QueryEvaluationException, RepositoryException {

        String queryString = "SELECT DISTINCT ?dataset WHERE {" +
                "?dataset " + VOID.SPARQLENDPOINT + " ?endpoint" +
                "?dataset " + VOID.PROPERTY + " ?p. }";

        RepositoryConnection conn = null;
        try {
            conn = voidRepository.getConnection();
            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            query.setBinding("endpoint", source);
            query.setBinding("p", propertyValue);

            return getDatasets(query.evaluate());

        } catch (RepositoryException e) {

        } catch (MalformedQueryException e) {

        } finally {
            if (conn != null)
                conn.close();
        }

        return new HashSet<Resource>();
    }

    private Set<Resource> getDatasetsOfEndpoint(URI source)
            throws QueryEvaluationException, RepositoryException {

        String queryString = "SELECT DISTINCT ?dataset WHERE {" +
                "?dataset " + VOID.SPARQLENDPOINT + " ?endpoint. }";

        RepositoryConnection conn = null;
        try {
            conn = voidRepository.getConnection();
            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            query.setBinding("endpoint", source);

            return getDatasets(query.evaluate());

        } catch (RepositoryException e) {

        } catch (MalformedQueryException e) {

        } finally {
            if (conn != null)
                conn.close();
        }

        return new HashSet<Resource>();
    }

    private Set<Resource> getDatasets(TupleQueryResult result) throws QueryEvaluationException {
        Set<Resource> set = new HashSet<Resource>();
        while (result.hasNext()){
            Value bindingValue = result.next().getBinding("dataset").getValue();
            if (bindingValue instanceof Resource)
                set.add((Resource)bindingValue);
        }
        return set;
    }

    public long getPatternCount(StatementPattern pattern, URI source) {

        Value sVal = pattern.getSubjectVar().getValue();
        Value pVal = pattern.getPredicateVar().getValue();
        Value oVal = pattern.getObjectVar().getValue();

        // case (s p o)
        if (sVal != null && pVal != null && oVal != null)
            return 1;

        // cases (s p O) || (S p O) || (S p o)
        if (pVal != null)
        {

            if (pVal != null) {
                String s = "SELECT ?triples { " +
                        "[] <" + VOID.PROPERTY + "> ?property ; " +
                        "   <" + VOID.SPARQLENDPOINT + "> ?endpoint ; " +
                        "   <" + VOID.TRIPLES +"> ?triples . }";

                RepositoryConnection conn = null;

                try {
                    conn = voidRepository.getConnection();
                    TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, s);
                    q.setIncludeInferred(true);
                    q.setBinding("property", pVal);
                    q.setBinding("endpoint", source);
                    TupleQueryResult result = q.evaluate();
                    if (result.hasNext()) {
                        return Long.parseLong(result.next().getBinding("triples").getValue().stringValue());
                    }
                    return 0;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (conn != null)
                        try { conn.close(); }catch(Exception e){ }
                }
            }


            if (sVal != null) { // case (s p O)

            } else { // case (S p O) || (S p o)

            }
        }

        // cases (s P o) || (s P O)
        if (sVal != null) {

        }

        // case (S P o)
        if (oVal != null) {


        } else { // case (S P O)

        }

        return 10;
    }


}
