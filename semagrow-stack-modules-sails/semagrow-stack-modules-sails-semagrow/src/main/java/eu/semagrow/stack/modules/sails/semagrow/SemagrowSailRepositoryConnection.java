package eu.semagrow.stack.modules.sails.semagrow;

import eu.semagrow.stack.modules.api.query.SemagrowQuery;
import eu.semagrow.stack.modules.api.repository.SemagrowRepositoryConnection;
import eu.semagrow.stack.modules.sails.semagrow.query.SemagrowSailBooleanQuery;
import eu.semagrow.stack.modules.sails.semagrow.query.SemagrowSailTupleQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.*;
import org.openrdf.repository.sail.*;

/**
 * Created by angel on 6/10/14.
 */
public class SemagrowSailRepositoryConnection extends SailRepositoryConnection
        implements SemagrowRepositoryConnection {

    public SemagrowSailRepositoryConnection(SemagrowSailRepository repository,
                                            SemagrowSailConnection sailConnection) {
        super(repository, sailConnection);
    }

    @Override
    public SailQuery prepareQuery(QueryLanguage ql, String queryString, String baseURI)
            throws MalformedQueryException {
        ParsedQuery parsedQuery = QueryParserUtil.parseQuery(ql, queryString, baseURI);

        if (parsedQuery instanceof ParsedTupleQuery) {
            return new SemagrowSailTupleQuery((ParsedTupleQuery)parsedQuery, this);
        }
        else if (parsedQuery instanceof ParsedBooleanQuery) {
            return new SemagrowSailBooleanQuery((ParsedBooleanQuery)parsedQuery, this);
        }
        else {
            throw new RuntimeException("Unexpected query type: " + parsedQuery.getClass());
        }
    }

    public SailQuery prepareQuery(QueryLanguage ql, String queryString)
            throws MalformedQueryException {
        return prepareQuery(ql, queryString, null);
    }

    public SemagrowSailTupleQuery prepareTupleQuery(QueryLanguage ql, String queryString, String baseURI)
            throws MalformedQueryException {
        SailTupleQuery q = super.prepareTupleQuery(ql, queryString,baseURI);
        return new SemagrowSailTupleQuery(q.getParsedQuery(), this);
    }

    public SemagrowSailBooleanQuery prepareBooleanQuery(QueryLanguage ql, String queryString, String baseURI)
            throws MalformedQueryException {
        SailBooleanQuery q = super.prepareBooleanQuery(ql, queryString, baseURI);
        return new SemagrowSailBooleanQuery(q.getParsedQuery(), this);
    }

    public SemagrowSailTupleQuery prepareTupleQuery(QueryLanguage ql, String queryString) throws MalformedQueryException {
        return prepareTupleQuery(ql, queryString, null);
    }

    public SemagrowSailBooleanQuery prepareBooleanQuery(QueryLanguage ql, String queryString) throws MalformedQueryException {
        return prepareBooleanQuery(ql, queryString, null);
    }
}
