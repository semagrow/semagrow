package eu.semagrow.stack.modules.sails.semagrow;

import eu.semagrow.stack.modules.api.repository.SemagrowRepositoryConnection;
import eu.semagrow.stack.modules.sails.semagrow.query.SemagrowSailTupleQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
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

    public SemagrowSailTupleQuery prepareTupleQuery(QueryLanguage ql, String queryString) throws MalformedQueryException {

        return prepareTupleQuery(ql, queryString, null);
    }

    public SemagrowSailTupleQuery prepareTupleQuery(QueryLanguage ql, String queryString, String baseURI)
            throws MalformedQueryException {
        SailTupleQuery q = super.prepareTupleQuery(ql, queryString,baseURI);
        return new SemagrowSailTupleQuery(q.getParsedQuery(), this);
    }
}
