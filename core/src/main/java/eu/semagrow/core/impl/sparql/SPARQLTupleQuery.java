package eu.semagrow.core.impl.sparql;

import org.eclipse.rdf4j.http.client.SparqlSession;
import org.eclipse.rdf4j.http.client.query.AbstractHTTPQuery;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.RepositoryException;

import java.io.IOException;


/**
 * Parses tuple results in the background.
 *
 * @author James Leigh
 */
public class SPARQLTupleQuery extends AbstractHTTPQuery implements TupleQuery {

    // TODO there was some magic going on in SparqlOperation to get baseURI
    // directly replaced within the query using BASE

    public SPARQLTupleQuery(SparqlSession httpClient, String baseUri, String queryString) {
        super(httpClient, QueryLanguage.SPARQL, queryString, baseUri);
    }

    public TupleQueryResult evaluate()
            throws QueryEvaluationException
    {

        SparqlSession client = getHttpClient();
        try {
            return client.sendTupleQuery(QueryLanguage.SPARQL, getQueryString(), baseURI, dataset,
                    getIncludeInferred(), getMaxExecutionTime(), getBindingsArray());
        }
        catch (IOException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
        catch (RepositoryException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
        catch (MalformedQueryException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
    }

    public void evaluate(TupleQueryResultHandler handler)
            throws QueryEvaluationException, TupleQueryResultHandlerException
    {

        SparqlSession client = getHttpClient();
        try {
            client.sendTupleQuery(QueryLanguage.SPARQL, getQueryString(), baseURI, dataset,
                    getIncludeInferred(), getMaxExecutionTime(), handler, getBindingsArray());
        }
        catch (IOException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
        catch (RepositoryException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
        catch (MalformedQueryException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
    }

    private String getQueryString() {
        return SPARQLQueryStringUtil.getQueryString(queryString, getBindings());
    }
}