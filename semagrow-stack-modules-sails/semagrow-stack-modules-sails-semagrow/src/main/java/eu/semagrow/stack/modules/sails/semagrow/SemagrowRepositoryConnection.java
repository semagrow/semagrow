package eu.semagrow.stack.modules.sails.semagrow;

import eu.semagrow.stack.modules.sails.semagrow.query.SemagrowSailTupleQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.*;
import org.openrdf.repository.sail.*;
import org.openrdf.sail.SailConnection;

/**
 * Created by angel on 6/10/14.
 */
public class SemagrowRepositoryConnection extends SailRepositoryConnection {

    public SemagrowRepositoryConnection(SemagrowRepository repository,
                                        SemagrowSailConnection sailConnection) {
        super(repository, sailConnection);
    }

    @Override
    public SemagrowSailTupleQuery prepareTupleQuery(QueryLanguage ql, String queryString) throws MalformedQueryException {

        return prepareTupleQuery(ql, queryString, null);
    }

    @Override
    public SemagrowSailTupleQuery prepareTupleQuery(QueryLanguage ql, String queryString, String baseURI)
            throws MalformedQueryException {
        SailTupleQuery q = super.prepareTupleQuery(ql, queryString,baseURI);
        return new SemagrowSailTupleQuery(q.getParsedQuery(), this);
    }
}
