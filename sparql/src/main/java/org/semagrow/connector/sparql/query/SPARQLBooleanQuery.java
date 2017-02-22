package org.semagrow.connector.sparql.query;

import org.semagrow.connector.sparql.query.render.SPARQLQueryStringUtil;
import org.eclipse.rdf4j.http.client.SparqlSession;
import org.eclipse.rdf4j.http.client.query.AbstractHTTPQuery;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.RepositoryException;

import java.io.IOException;

/**
 * Parses boolean query response from remote stores.
 *
 * @author James Leigh
 *
 */
public class SPARQLBooleanQuery extends AbstractHTTPQuery implements BooleanQuery {

    public SPARQLBooleanQuery(SparqlSession httpClient, String baseURI,
                              String queryString) {
        super(httpClient, QueryLanguage.SPARQL, queryString, baseURI);
    }

    public boolean evaluate() throws QueryEvaluationException {

        SparqlSession client = getHttpClient();

        try {
            return client.sendBooleanQuery(queryLanguage, getQueryString(), baseURI, dataset, getIncludeInferred(), getMaxExecutionTime(),
                    getBindingsArray());
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
