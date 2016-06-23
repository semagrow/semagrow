package org.semagrow.connector.sparql.query;

import org.semagrow.connector.sparql.query.render.SPARQLQueryStringUtil;
import org.eclipse.rdf4j.http.client.SparqlSession;
import org.eclipse.rdf4j.http.client.query.AbstractHTTPQuery;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;

import java.io.IOException;


/**
 * Parses RDF results in the background.
 *
 * @author James Leigh
 * @author Andreas Schwarte
 */
public class SPARQLGraphQuery extends AbstractHTTPQuery implements GraphQuery {

    public SPARQLGraphQuery(SparqlSession httpClient, String baseURI, String queryString) {
        super(httpClient, QueryLanguage.SPARQL, queryString, baseURI);
    }

    public GraphQueryResult evaluate()
            throws QueryEvaluationException
    {
        SparqlSession client = getHttpClient();
        try {
            // TODO getQueryString() already inserts bindings, use emptybindingset
            // as last argument?
            return client.sendGraphQuery(queryLanguage, getQueryString(), baseURI, dataset,
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

    public void evaluate(RDFHandler handler)
            throws QueryEvaluationException, RDFHandlerException
    {

        SparqlSession client = getHttpClient();
        try {
            client.sendGraphQuery(queryLanguage, getQueryString(), baseURI, dataset, getIncludeInferred(),
                    getMaxExecutionTime(), handler, getBindingsArray());
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
