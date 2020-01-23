package org.semagrow.connector.sparql.execution;

import org.eclipse.rdf4j.http.client.SPARQLProtocolSession;
import org.semagrow.connector.sparql.query.SPARQLTupleQuery;
import org.semagrow.connector.sparql.query.SPARQLBooleanQuery;
import org.semagrow.connector.sparql.query.SPARQLGraphQuery;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.RepositoryException;


/**
 * Created by angel on 9/6/2016.
 */
public class SPARQLConnection extends org.eclipse.rdf4j.repository.sparql.SPARQLConnection {

    private final SPARQLProtocolSession client;

    public SPARQLConnection(SPARQLRepository repository, SPARQLProtocolSession client, boolean quadMode) {
        super(repository, client, quadMode);
        this.client = client;

    }

    @Override
    public Query prepareQuery(QueryLanguage ql, String query, String base)
            throws RepositoryException, MalformedQueryException
    {
        if (QueryLanguage.SPARQL.equals(ql)) {
            String strippedQuery = QueryParserUtil.removeSPARQLQueryProlog(query).toUpperCase();
            if (strippedQuery.startsWith("SELECT")) {
                return prepareTupleQuery(ql, query, base);
            }
            else if (strippedQuery.startsWith("ASK")) {
                return prepareBooleanQuery(ql, query, base);
            }
            else {
                return prepareGraphQuery(ql, query, base);
            }
        }
        throw new UnsupportedOperationException("Unsupported query language " + ql);
    }

    @Override
    public BooleanQuery prepareBooleanQuery(QueryLanguage ql, String query, String base)
            throws RepositoryException, MalformedQueryException
    {
        if (QueryLanguage.SPARQL.equals(ql)) {
            return new SPARQLBooleanQuery(client, base, query);
        }
        throw new UnsupportedQueryLanguageException("Unsupported query language " + ql);
    }

    @Override
    public GraphQuery prepareGraphQuery(QueryLanguage ql, String query, String base)
            throws RepositoryException, MalformedQueryException
    {
        if (QueryLanguage.SPARQL.equals(ql)) {
            return new SPARQLGraphQuery(client, base, query);
        }
        throw new UnsupportedQueryLanguageException("Unsupported query language " + ql);
    }

    @Override
    public TupleQuery prepareTupleQuery(QueryLanguage ql, String query, String base)
            throws RepositoryException, MalformedQueryException
    {
        if (QueryLanguage.SPARQL.equals(ql))
            return new SPARQLTupleQuery(client, base, query);
        throw new UnsupportedQueryLanguageException("Unsupported query language " + ql);
    }

}
