package org.semagrow.connector.postgis.execution;

import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.ParsedQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.rdf4j.queryrender.QueryRenderer;

import java.util.Collections;


public class PostGISQueryRenderer implements QueryRenderer {
	
	
	private static final Logger logger = LoggerFactory.getLogger(PostGISQueryRenderer.class);
	
	
	private static QueryLanguage SQL = new QueryLanguage("SQL");

    public QueryLanguage getLanguage() {
    	return SQL;
    }

    public String render(ParsedQuery parsedQuery) throws Exception {
    	logger.debug("PostGISQueryRenderer");
    	logger.warn("PostGISQueryRenderer");
    	logger.info("PostGISQueryRenderer");
    	PostGISQueryTransformer transformer = new PostGISQueryTransformer();
        return transformer.transformQuery(null, null, parsedQuery.getTupleExpr(), Collections.emptyList());
    }
}
