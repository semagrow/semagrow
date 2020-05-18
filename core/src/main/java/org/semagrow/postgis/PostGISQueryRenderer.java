package org.semagrow.postgis;

import java.util.Collections;

import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.queryrender.QueryRenderer;


public class PostGISQueryRenderer implements QueryRenderer {	
	
	private static QueryLanguage SQL = new QueryLanguage("SQL");

    public QueryLanguage getLanguage() {
    	return SQL;
    }

    public String render(ParsedQuery parsedQuery) throws Exception {
    	PostGISQueryTransformer transformer = new PostGISQueryTransformer();
        return transformer.transformQuery(null, null, parsedQuery.getTupleExpr(), Collections.emptyList());
    }
}
