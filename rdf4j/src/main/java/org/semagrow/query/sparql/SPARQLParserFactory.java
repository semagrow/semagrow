package org.semagrow.query.sparql;

import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.QueryParserFactory;

/**
 * Created by angel on 8/6/2016.
 */
public class SPARQLParserFactory implements QueryParserFactory {


    private static QueryParser sparqlParser = new SPARQLParser();

    @Override
    public QueryLanguage getQueryLanguage() {
        return QueryLanguage.SPARQL;
    }

    @Override
    public QueryParser getParser() {
        return sparqlParser;
    }

}
