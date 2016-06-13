package eu.semagrow.repository;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;

/**
 * Created by angel on 6/11/14.
 */
public interface SemagrowRepository extends Repository {

    SemagrowRepositoryConnection getConnection() throws RepositoryException;

    /*
    void setSourceSelector(SourceSelector sourceSelector);

    SourceSelector getSourceSelector();

    void setQueryEvaluation(FederatedQueryEvaluation queryEvaluation);
    */

    Repository getMetadataRepository();
}
