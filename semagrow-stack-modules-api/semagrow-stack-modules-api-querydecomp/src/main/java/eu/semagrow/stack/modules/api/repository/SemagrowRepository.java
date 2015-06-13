package eu.semagrow.stack.modules.api.repository;

import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;

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
