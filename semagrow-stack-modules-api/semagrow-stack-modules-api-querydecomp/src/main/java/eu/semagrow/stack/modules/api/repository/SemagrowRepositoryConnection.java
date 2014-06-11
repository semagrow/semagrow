package eu.semagrow.stack.modules.api.repository;

import eu.semagrow.stack.modules.api.query.SemagrowTupleQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;

/**
 * Created by angel on 6/11/14.
 */
public interface SemagrowRepositoryConnection extends RepositoryConnection {

    SemagrowTupleQuery prepareTupleQuery(QueryLanguage ql, String queryString)
            throws MalformedQueryException;

    SemagrowTupleQuery prepareTupleQuery(QueryLanguage ql, String queryString, String baseURI)
            throws MalformedQueryException;
}
