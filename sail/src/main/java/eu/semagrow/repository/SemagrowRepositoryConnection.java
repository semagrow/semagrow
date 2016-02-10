package eu.semagrow.repository;

import eu.semagrow.query.SemagrowTupleQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

/**
 * Created by angel on 6/11/14.
 */
public interface SemagrowRepositoryConnection extends RepositoryConnection {

    SemagrowTupleQuery prepareTupleQuery(QueryLanguage ql, String queryString)
            throws MalformedQueryException, RepositoryException;

    SemagrowTupleQuery prepareTupleQuery(QueryLanguage ql, String queryString, String baseURI)
            throws MalformedQueryException, RepositoryException;
}
