package eu.semagrow.cassandra.eval;

import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.queryrender.QueryRenderer;

import java.util.Collections;

/**
 * Created by angel on 21/4/2016.
 */
public class CQLQueryRenderer implements QueryRenderer {

    private static QueryLanguage CQL = new QueryLanguage("CQL");

    public QueryLanguage getLanguage() {
        return CQL;
    }

    public String render(ParsedQuery parsedQuery) throws Exception {
        CassandraQueryTransformer transformer = new CassandraQueryTransformer();
        return transformer.transformQuery(null, parsedQuery.getTupleExpr(), Collections.emptyList());
    }
}
