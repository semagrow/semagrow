package org.semagrow.repository;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;

/**
 * Created by angel on 6/11/14.
 */
public interface SemagrowRepository extends Repository {

    SemagrowRepositoryConnection getConnection() throws RepositoryException;

    Repository getMetadataRepository();
}
