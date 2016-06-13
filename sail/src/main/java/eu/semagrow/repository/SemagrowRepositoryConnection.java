package eu.semagrow.repository;

import eu.semagrow.query.SemagrowTupleQuery;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;

/**
 * Created by angel on 6/11/14.
 */
public interface SemagrowRepositoryConnection extends RepositoryConnection {

    SemagrowTupleQuery prepareTupleQuery(QueryLanguage ql, String queryString)
            throws MalformedQueryException, RepositoryException;

    SemagrowTupleQuery prepareTupleQuery(QueryLanguage ql, String queryString, String baseURI)
            throws MalformedQueryException, RepositoryException;
}
